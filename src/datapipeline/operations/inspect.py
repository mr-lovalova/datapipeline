import io
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional

from datapipeline.analysis.vector.collector import VectorStatsCollector
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import OperationTask
from datapipeline.dag.context import PipelineContext
from datapipeline.pipelines import build_vector_pipeline
from datapipeline.pipelines.full.nodes import post_process
from datapipeline.runtime import Runtime
from datapipeline.utils.paths import ensure_parent

def _option(options: Mapping[str, Any], key: str, default: Any) -> Any:
    value = options.get(key, default)
    return default if value is None else value


def _merge_sample_values(sample) -> dict:
    merged = dict(sample.features.values)
    if sample.targets:
        merged.update(sample.targets.values)
    return merged


def _iter_merged_vectors(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    *,
    apply_postprocess: bool,
) -> Iterator[tuple[object, dict]]:
    context = PipelineContext(runtime)
    feature_cfgs = list(dataset.features or [])
    target_cfgs = list(dataset.targets or [])

    context.window_bounds(rectangular_required=True)
    vectors = build_vector_pipeline(
        context,
        feature_cfgs,
        dataset.group_by,
        target_configs=target_cfgs,
        rectangular=True,
    )
    if apply_postprocess:
        vectors = post_process(context, vectors)

    for sample in vectors:
        yield sample.key, _merge_sample_values(sample)


def _resolve_output_path(
    runtime: Runtime,
    *,
    target,
    options: Mapping[str, Any],
    default_filename: str,
) -> Path:
    def _resolve(candidate: Path) -> Path:
        if candidate.is_absolute():
            return candidate
        return (runtime.artifacts_root / candidate).resolve()

    explicit = options.get("output")
    if isinstance(explicit, str) and explicit.strip():
        return _resolve(Path(explicit.strip()))
    destination = getattr(target, "destination", None)
    if destination is not None:
        return _resolve(Path(destination))
    return (runtime.artifacts_root / "inspect" / default_filename).resolve()


def _run_report(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    *,
    options: Mapping[str, Any],
    matrix_format: str | None = None,
    matrix_path: Path | None = None,
) -> None:
    expected_feature_ids = [cfg.id for cfg in (dataset.features or [])]
    schema_entries = PipelineContext(runtime).load_schema(payload="features")
    schema_meta = {
        entry["id"]: entry
        for entry in (schema_entries or [])
        if isinstance(entry.get("id"), str)
    }

    collector = VectorStatsCollector(
        expected_feature_ids or None,
        match_partition=str(_option(options, "match_partition", "base")),
        schema_meta=schema_meta,
        threshold=float(_option(options, "threshold", 0.95)),
        show_matrix=False,
        matrix_rows=int(_option(options, "rows", 20)),
        matrix_cols=int(_option(options, "cols", 10)),
        matrix_output=(str(matrix_path) if matrix_path is not None else None),
        matrix_format=(matrix_format or "html"),
    )

    mode_value = str(_option(options, "mode", "final")).lower()
    apply_postprocess = mode_value != "raw"
    for key, merged in _iter_merged_vectors(
        runtime,
        dataset,
        apply_postprocess=apply_postprocess,
    ):
        collector.update(key, merged)

    report_buffer = io.StringIO()
    with redirect_stdout(report_buffer):
        collector.print_report(sort_key=str(_option(options, "sort", "missing")))

    quiet = bool(_option(options, "quiet", False))
    if not quiet:
        report_text = report_buffer.getvalue()
        if report_text.strip():
            print(report_text, end="")


def inspect_report_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int] = None,
    target=None,
    throttle_ms: Optional[float] = None,
    stage: Optional[int] = None,
    visuals: Optional[str] = None,
    operation_task: OperationTask | None = None,
) -> None:
    _ = limit, target, throttle_ms, stage, visuals
    options = operation_task.options if operation_task is not None else {}
    _run_report(runtime, dataset, options=options)


def inspect_matrix_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int] = None,
    target=None,
    throttle_ms: Optional[float] = None,
    stage: Optional[int] = None,
    visuals: Optional[str] = None,
    operation_task: OperationTask | None = None,
) -> None:
    _ = limit, throttle_ms, stage, visuals
    options = operation_task.options if operation_task is not None else {}
    matrix_format = str(_option(options, "format", "html")).lower()
    if matrix_format not in {"csv", "html"}:
        raise ValueError(
            f"Invalid inspect matrix format '{matrix_format}'. Use 'csv' or 'html'."
        )
    default_name = "matrix.csv" if matrix_format == "csv" else "matrix.html"
    matrix_path = _resolve_output_path(
        runtime,
        target=target,
        options=options,
        default_filename=default_name,
    )
    ensure_parent(matrix_path)
    _run_report(
        runtime,
        dataset,
        options=options,
        matrix_format=matrix_format,
        matrix_path=matrix_path,
    )
