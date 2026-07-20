import shutil
from collections.abc import Sequence

from datapipeline.artifacts.variable_records import (
    VARIABLE_RECORDS_MANIFEST_VERSION,
    variable_record_to_row,
    write_variable_rows,
)
from datapipeline.artifacts.specs import VARIABLE_RECORDS
from datapipeline.config.dataset.variable import VariableConfig
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.execution.context import PipelineContext
from datapipeline.pipelines.variable.pipeline import run_variable_pipeline
from datapipeline.runtime import Runtime
from datapipeline.services.path_policy import sanitize_path_segment
from datapipeline.utils.json_artifact import write_json_artifact


def register_variable_records(
    runtime: Runtime,
    features: Sequence[VariableConfig],
    cadence: str,
    *,
    targets: Sequence[VariableConfig] = (),
    sample_keys: Sequence[str] = (),
) -> None:
    relative_path = "build/variable_records/manifest.json"
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
            "version": VARIABLE_RECORDS_MANIFEST_VERSION,
            "format": "jsonl.gz",
            "cadence": cadence,
            "sample_keys": list(sample_keys),
            "sample_key_types": list(sample_key_contract.types),
            "features": feature_shards,
            "targets": target_shards,
        },
    )
    runtime.artifacts.register(VARIABLE_RECORDS, relative_path=relative_path)


def _write_shards(
    context: PipelineContext,
    root,
    directory: str,
    configs: Sequence[VariableConfig],
    cadence: str,
    sample_key_contract: SampleKeyContract,
) -> list[dict[str, object]]:
    shards: list[dict[str, object]] = []
    for cfg in configs:
        file_name = f"{sanitize_path_segment(cfg.id)}.jsonl.gz"
        relative_path = f"{directory}/{file_name}"
        stream = run_variable_pipeline(
            context,
            cfg,
            sample_keys=sample_key_contract.fields,
            group_by_cadence=cadence,
        )
        try:

            def rows():
                for item in stream:
                    sample_key_contract.validate(item.entity_key)
                    yield variable_record_to_row(item)

            written = write_variable_rows(root / relative_path, rows())
        finally:
            closer = getattr(stream, "close", None)
            if callable(closer):
                closer()
        shards.append({"id": cfg.id, "path": relative_path, "rows": written})
    return shards
