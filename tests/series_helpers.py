import shutil
from collections.abc import Sequence

from datapipeline.artifacts.series import (
    SERIES_MANIFEST_VERSION,
    series_record_to_row,
    write_series_rows,
)
from datapipeline.artifacts.specs import SERIES
from datapipeline.config.dataset.series import SeriesConfig
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.execution.context import PipelineContext
from datapipeline.pipelines.series.pipeline import run_series_pipeline
from datapipeline.runtime import Runtime
from datapipeline.services.path_policy import sanitize_path_segment
from datapipeline.utils.json_artifact import write_json_artifact


def register_series(
    runtime: Runtime,
    features: Sequence[SeriesConfig],
    cadence: str,
    *,
    targets: Sequence[SeriesConfig] = (),
    sample_keys: Sequence[str] = (),
) -> None:
    relative_path = "build/series/manifest.json"
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
            "version": SERIES_MANIFEST_VERSION,
            "format": "jsonl.gz",
            "cadence": cadence,
            "sample_keys": list(sample_keys),
            "sample_key_types": list(sample_key_contract.types),
            "features": feature_shards,
            "targets": target_shards,
        },
    )
    runtime.artifacts.register(SERIES, relative_path=relative_path)


def _write_shards(
    context: PipelineContext,
    root,
    directory: str,
    configs: Sequence[SeriesConfig],
    cadence: str,
    sample_key_contract: SampleKeyContract,
) -> list[dict[str, object]]:
    shards: list[dict[str, object]] = []
    for cfg in configs:
        file_name = f"{sanitize_path_segment(cfg.id)}.jsonl.gz"
        relative_path = f"{directory}/{file_name}"
        stream = run_series_pipeline(
            context,
            cfg,
            sample_keys=sample_key_contract.fields,
            group_by_cadence=cadence,
        )
        try:

            def rows():
                for item in stream:
                    sample_key_contract.validate(item.entity_key)
                    yield series_record_to_row(item)

            written = write_series_rows(root / relative_path, rows())
        finally:
            closer = getattr(stream, "close", None)
            if callable(closer):
                closer()
        shards.append({"id": cfg.id, "path": relative_path, "rows": written})
    return shards
