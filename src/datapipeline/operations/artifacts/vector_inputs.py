import shutil
from collections.abc import Sequence
from pathlib import Path

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.dataset.validation import validate_dataset_feature_identity
from datapipeline.config.tasks import VectorInputsTask
from datapipeline.execution.context import PipelineContext
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.pipelines.feature.pipeline import run_feature_pipeline
from datapipeline.runtime import Runtime
from datapipeline.utils.json_artifact import write_json_artifact
from datapipeline.vector_inputs.store import (
    CachedVectorInputShard,
    CachedVectorInputsManifest,
    VECTOR_INPUTS_MANIFEST_VERSION,
    feature_record_to_vector_input_row,
    write_vector_input_rows,
)


def materialize_vector_inputs(
    runtime: Runtime,
    task_cfg: VectorInputsTask,
) -> ArtifactOutput:
    dataset = load_dataset(runtime.project_yaml)
    validate_dataset_feature_identity(runtime, dataset)

    relative_path = Path(task_cfg.output)
    artifacts_root = Path(runtime.artifacts_root).resolve()
    destination = artifacts_root / relative_path
    resolved_destination = destination.resolve()
    try:
        resolved_destination.relative_to(artifacts_root)
    except ValueError as exc:
        raise ValueError(
            f"Vector inputs output must stay under artifacts root '{artifacts_root}'."
        ) from exc
    if resolved_destination != destination:
        raise ValueError("Vector inputs output must not resolve through a symlink.")
    cache_root = destination.parent / f"{destination.stem}.shards"
    if cache_root.is_symlink():
        raise ValueError("Vector inputs shard directory must not be a symlink.")
    _clear_vector_inputs_cache(cache_root, destination)

    context = PipelineContext(runtime)
    sample_key_contract = SampleKeyContract(dataset.sample.keys)
    feature_shards = _materialize_shards(
        context=context,
        cache_root=cache_root,
        shard_dir_name="features",
        configs=dataset.features,
        sample_key_contract=sample_key_contract,
        cadence=dataset.sample.cadence,
    )
    target_shards = _materialize_shards(
        context=context,
        cache_root=cache_root,
        shard_dir_name="targets",
        configs=dataset.targets,
        sample_key_contract=sample_key_contract,
        cadence=dataset.sample.cadence,
    )

    manifest = CachedVectorInputsManifest(
        version=VECTOR_INPUTS_MANIFEST_VERSION,
        cadence=dataset.sample.cadence,
        sample_keys=tuple(dataset.sample.keys),
        sample_key_types=sample_key_contract.types,
        features=feature_shards,
        targets=target_shards,
    )
    write_json_artifact(destination, manifest.model_dump(mode="json"))

    feature_rows = sum(shard.rows for shard in feature_shards)
    target_rows = sum(shard.rows for shard in target_shards)
    return ArtifactOutput(
        relative_path=str(relative_path),
        companion_paths=tuple(
            str(relative_path.parent / shard.path)
            for shard in (*feature_shards, *target_shards)
        ),
        meta={
            "features": len(feature_shards),
            "targets": len(target_shards),
            "feature_rows": feature_rows,
            "target_rows": target_rows,
            "format": manifest.format,
        },
    )


def _clear_vector_inputs_cache(cache_root: Path, manifest_path: Path) -> None:
    if cache_root.exists():
        shutil.rmtree(cache_root)
    manifest_path.unlink(missing_ok=True)


def _materialize_shards(
    *,
    context: PipelineContext,
    cache_root: Path,
    shard_dir_name: str,
    configs: Sequence[FeatureRecordConfig],
    sample_key_contract: SampleKeyContract,
    cadence: str,
) -> tuple[CachedVectorInputShard, ...]:
    shard_dir = cache_root / shard_dir_name
    shard_dir.mkdir(parents=True, exist_ok=True)
    shards: list[CachedVectorInputShard] = []
    for index, cfg in enumerate(configs):
        file_name = f"{index:06d}.jsonl.gz"
        path = shard_dir / file_name
        rows = _write_feature_config(
            context=context,
            path=path,
            cfg=cfg,
            sample_key_contract=sample_key_contract,
            cadence=cadence,
        )
        shards.append(
            CachedVectorInputShard(
                id=cfg.id,
                path=f"{cache_root.name}/{shard_dir_name}/{file_name}",
                rows=rows,
            )
        )
    return tuple(shards)


def _write_feature_config(
    *,
    context: PipelineContext,
    path: Path,
    cfg: FeatureRecordConfig,
    sample_key_contract: SampleKeyContract,
    cadence: str,
) -> int:
    features = run_feature_pipeline(
        context,
        cfg,
        sample_keys=sample_key_contract.fields,
        group_by_cadence=cadence,
    )
    try:

        def rows():
            for item in features:
                sample_key_contract.validate(item.entity_key)
                yield feature_record_to_vector_input_row(item)

        return write_vector_input_rows(path, rows())
    finally:
        closer = getattr(features, "close", None)
        if callable(closer):
            closer()
