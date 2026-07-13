import shutil
from collections.abc import Sequence

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.execution.context import PipelineContext
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.pipelines.feature.pipeline import run_feature_pipeline
from datapipeline.runtime import Runtime
from datapipeline.services.constants import VECTOR_INPUTS
from datapipeline.services.path_policy import sanitize_path_segment
from datapipeline.utils.json_artifact import write_json_artifact
from datapipeline.vector_inputs.store import (
    VECTOR_INPUTS_MANIFEST_VERSION,
    feature_record_to_vector_input_row,
    write_vector_input_rows,
)


def register_vector_inputs(
    runtime: Runtime,
    features: Sequence[FeatureRecordConfig],
    cadence: str,
    *,
    targets: Sequence[FeatureRecordConfig] = (),
    sample_keys: Sequence[str] = (),
) -> None:
    relative_path = "build/vector_inputs/manifest.json"
    manifest_path = runtime.artifacts_root / relative_path
    root = manifest_path.parent
    shutil.rmtree(root, ignore_errors=True)
    context = PipelineContext(runtime)
    sample_key_contract = SampleKeyContract(sample_keys)
    feature_shards = _write_shards(
        context,
        root,
        "features",
        features,
        cadence,
        sample_key_contract,
    )
    target_shards = _write_shards(
        context,
        root,
        "targets",
        targets,
        cadence,
        sample_key_contract,
    )
    write_json_artifact(
        manifest_path,
        {
            "version": VECTOR_INPUTS_MANIFEST_VERSION,
            "format": "jsonl.gz",
            "cadence": cadence,
            "sample_keys": list(sample_keys),
            "sample_key_types": list(sample_key_contract.types),
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
    cadence: str,
    sample_key_contract: SampleKeyContract,
) -> list[dict[str, object]]:
    shards: list[dict[str, object]] = []
    for cfg in configs:
        file_name = f"{sanitize_path_segment(cfg.id)}.jsonl.gz"
        relative_path = f"{directory}/{file_name}"
        stream = run_feature_pipeline(
            context,
            cfg,
            sample_keys=sample_key_contract.fields,
            group_by_cadence=cadence,
        )
        try:

            def rows():
                for item in stream:
                    sample_key_contract.validate(item.entity_key)
                    yield feature_record_to_vector_input_row(item)

            count = write_vector_input_rows(root / relative_path, rows())
        finally:
            closer = getattr(stream, "close", None)
            if callable(closer):
                closer()
        shards.append({"id": cfg.id, "path": relative_path, "rows": count})
    return shards
