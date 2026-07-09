import shutil
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.dataset.validation import validate_dataset_feature_identity
from datapipeline.config.tasks import VectorInputsTask
from datapipeline.dag.context import PipelineContext
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.pipelines.feature.dag import build_feature_pipeline
from datapipeline.runtime import Runtime
from datapipeline.services.path_policy import sanitize_path_segment
from datapipeline.utils.json_artifact import write_json_artifact
from datapipeline.vector_inputs import (
    feature_record_to_vector_input_row,
    write_vector_input_rows,
)


def materialize_vector_inputs(
    runtime: Runtime,
    task_cfg: VectorInputsTask,
) -> ArtifactOutput:
    dataset = load_dataset(runtime.project_yaml, "vectors")
    validate_dataset_feature_identity(runtime, dataset)
    if task_cfg.format != "jsonl.gz":
        raise ValueError("vector_inputs currently supports format 'jsonl.gz'.")

    relative_path = Path(task_cfg.output)
    destination = (runtime.artifacts_root / relative_path).resolve()
    cache_root = destination.parent
    _clear_vector_inputs_cache(cache_root, destination)

    context = PipelineContext(runtime)
    runtime.sample_keys = list(dataset.sample_keys)
    feature_shards = _materialize_shards(
        context=context,
        cache_root=cache_root,
        shard_dir_name="features",
        configs=list(dataset.features or ()),
        sample_keys=dataset.sample_keys,
        group_by=dataset.group_by,
    )
    target_shards = _materialize_shards(
        context=context,
        cache_root=cache_root,
        shard_dir_name="targets",
        configs=list(dataset.targets or ()),
        sample_keys=dataset.sample_keys,
        group_by=dataset.group_by,
    )

    payload = {
        "version": 1,
        "format": task_cfg.format,
        "group_by": dataset.group_by,
        "sample_keys": list(dataset.sample_keys),
        "features": feature_shards,
        "targets": target_shards,
    }
    write_json_artifact(destination, payload)

    feature_rows = sum(int(item["rows"]) for item in feature_shards)
    target_rows = sum(int(item["rows"]) for item in target_shards)
    return ArtifactOutput(
        relative_path=str(relative_path),
        meta={
            "features": len(feature_shards),
            "targets": len(target_shards),
            "feature_rows": feature_rows,
            "target_rows": target_rows,
            "format": task_cfg.format,
        },
    )


def _clear_vector_inputs_cache(cache_root: Path, manifest_path: Path) -> None:
    shutil.rmtree(cache_root / "features", ignore_errors=True)
    shutil.rmtree(cache_root / "targets", ignore_errors=True)
    manifest_path.unlink(missing_ok=True)


def _materialize_shards(
    *,
    context: PipelineContext,
    cache_root: Path,
    shard_dir_name: str,
    configs: Sequence[FeatureRecordConfig],
    sample_keys: Sequence[str],
    group_by: str,
) -> list[dict[str, Any]]:
    shard_dir = cache_root / shard_dir_name
    shard_dir.mkdir(parents=True, exist_ok=True)
    used_paths: dict[str, str] = {}
    shards: list[dict[str, Any]] = []
    for cfg in configs:
        file_name = f"{sanitize_path_segment(cfg.id)}.jsonl.gz"
        existing = used_paths.get(file_name)
        if existing is not None:
            raise ValueError(
                f"Feature ids '{existing}' and '{cfg.id}' resolve to the same cache shard path."
            )
        used_paths[file_name] = cfg.id
        path = shard_dir / file_name
        rows = _write_feature_config(
            context=context,
            path=path,
            cfg=cfg,
            sample_keys=sample_keys,
            group_by=group_by,
        )
        shards.append(
            {
                "id": cfg.id,
                "path": f"{shard_dir_name}/{file_name}",
                "rows": rows,
            }
        )
    return shards


def _write_feature_config(
    *,
    context: PipelineContext,
    path: Path,
    cfg: FeatureRecordConfig,
    sample_keys: Sequence[str],
    group_by: str,
) -> int:
    features = build_feature_pipeline(
        context,
        cfg,
        sample_keys=sample_keys,
        group_by_cadence=group_by,
    )
    try:
        return write_vector_input_rows(
            path,
            (feature_record_to_vector_input_row(item) for item in features),
        )
    finally:
        closer = getattr(features, "close", None)
        if callable(closer):
            closer()
