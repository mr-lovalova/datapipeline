import shutil
from collections.abc import Sequence

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines.feature.dag import build_feature_pipeline
from datapipeline.runtime import Runtime
from datapipeline.services.constants import VECTOR_INPUTS
from datapipeline.services.path_policy import sanitize_path_segment
from datapipeline.utils.json_artifact import write_json_artifact
from datapipeline.vector_inputs import (
    feature_record_to_vector_input_row,
    write_vector_input_rows,
)


def register_vector_inputs(
    runtime: Runtime,
    features: Sequence[FeatureRecordConfig],
    group_by: str,
    *,
    targets: Sequence[FeatureRecordConfig] = (),
    sample_keys: Sequence[str] = (),
) -> None:
    relative_path = "build/vector_inputs/manifest.json"
    manifest_path = runtime.artifacts_root / relative_path
    root = manifest_path.parent
    shutil.rmtree(root, ignore_errors=True)
    context = PipelineContext(runtime)
    runtime.sample_keys = list(sample_keys)
    feature_shards = _write_shards(
        context,
        root,
        "features",
        features,
        group_by,
        sample_keys,
    )
    target_shards = _write_shards(
        context,
        root,
        "targets",
        targets,
        group_by,
        sample_keys,
    )
    write_json_artifact(
        manifest_path,
        {
            "version": 1,
            "format": "jsonl.gz",
            "group_by": group_by,
            "sample_keys": list(sample_keys),
            "features": feature_shards,
            "targets": target_shards,
        },
    )
    runtime.artifacts.register(VECTOR_INPUTS, relative_path=relative_path)


def _write_shards(
    context: PipelineContext,
    root,
    directory: str,
    configs: Sequence[FeatureRecordConfig],
    group_by: str,
    sample_keys: Sequence[str],
) -> list[dict[str, object]]:
    shards: list[dict[str, object]] = []
    for cfg in configs:
        file_name = f"{sanitize_path_segment(cfg.id)}.jsonl.gz"
        relative_path = f"{directory}/{file_name}"
        stream = build_feature_pipeline(
            context,
            cfg,
            sample_keys=sample_keys,
            group_by_cadence=group_by,
        )
        try:
            rows = (
                feature_record_to_vector_input_row(item)
                for item in stream
            )
            count = write_vector_input_rows(root / relative_path, rows)
        finally:
            closer = getattr(stream, "close", None)
            if callable(closer):
                closer()
        shards.append({"id": cfg.id, "path": relative_path, "rows": count})
    return shards
