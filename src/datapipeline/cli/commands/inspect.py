import io
import json
import logging
from contextlib import redirect_stdout
from pathlib import Path

from datapipeline.analysis.vector.collector import VectorStatsCollector
from datapipeline.cli.visuals.runner import run_job
from datapipeline.cli.visuals.execution import make_execution_observer
from datapipeline.config.context import load_dataset_context
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.utils.paths import ensure_parent
from datapipeline.services.bootstrap import artifacts_root
from datapipeline.pipelines.full.nodes import post_process
from datapipeline.pipelines import build_vector_pipeline
from datapipeline.artifacts.specs import StageDemand, required_artifacts_for
from datapipeline.cli.commands.build import run_build_if_needed


def _prepare_inspect_build(
    project: str | Path,
    *,
    visuals: str | None,
    workspace=None,
) -> None:
    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    demands = [StageDemand(stage=None)]
    required = required_artifacts_for(dataset, demands)
    if not required:
        return
    run_build_if_needed(
        project_path,
        required_artifacts=required,
        cli_visuals=visuals,
        workspace=workspace,
    )


def _run_inspect_job(
    project: str,
    *,
    visuals: str | None,
    log_level: int | None,
    label: str,
    section: str,
    work,
) -> None:
    dataset_ctx = load_dataset_context(project)
    level_value = log_level if log_level is not None else logging.getLogger().getEffectiveLevel()
    visuals_provider = visuals or "on"
    observer = make_execution_observer(
        logging.getLogger("datapipeline.dag.observer")
    )
    dataset_ctx.runtime.execution_observer = observer
    dataset_ctx.pipeline_context.execution_observer = observer

    run_job(
        sections=("inspect", section),
        label=label,
        visuals=visuals_provider,
        level=level_value,
        runtime=dataset_ctx.runtime,
        work=lambda: work(dataset_ctx),
    )


def _merge_sample_values(sample) -> dict:
    merged = dict(sample.features.values)
    if sample.targets:
        merged.update(sample.targets.values)
    return merged


def _iter_merged_vectors(
    dataset_ctx,
    *,
    apply_postprocess: bool,
):
    context = dataset_ctx.pipeline_context
    dataset = dataset_ctx.dataset
    feature_cfgs = list(dataset_ctx.features or [])
    target_cfgs = list(dataset_ctx.targets or [])

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


def report(
    project: str,
    *,
    threshold: float = 0.95,
    match_partition: str = "base",
    matrix: str = "none",  # one of: none|csv|html
    output: str | None = None,
    rows: int = 20,
    cols: int = 10,
    quiet: bool = False,
    apply_postprocess: bool = True,
    visuals: str | None = None,
    log_level: int | None = None,
    sort: str = "missing",
    workspace=None,
) -> None:
    """Compute a quality report and optionally export a matrix.

    - Always prints a human-readable report (unless quiet=True).
    - When matrix != 'none', writes an availability matrix in the requested format.
    """

    _prepare_inspect_build(
        project,
        visuals=visuals,
        workspace=workspace,
    )

    def _work(dataset_ctx):
        project_path = dataset_ctx.project

        feature_cfgs = dataset_ctx.features
        expected_feature_ids = [cfg.id for cfg in feature_cfgs]

        matrix_fmt = matrix if matrix in {"csv", "html"} else None
        if matrix_fmt:
            filename = "matrix.html" if matrix_fmt == "html" else "matrix.csv"
        else:
            filename = None
        base_artifacts = artifacts_root(project_path)
        matrix_path = None
        if matrix_fmt:
            matrix_path = Path(output) if output else (base_artifacts / filename)

        schema_entries = dataset_ctx.pipeline_context.load_schema(payload="features")
        schema_meta = {entry["id"]: entry for entry in (schema_entries or []) if isinstance(entry.get("id"), str)}

        collector = VectorStatsCollector(
            expected_feature_ids or None,
            match_partition=match_partition,
            schema_meta=schema_meta,
            threshold=threshold,
            show_matrix=False,
            matrix_rows=rows,
            matrix_cols=cols,
            matrix_output=(str(matrix_path) if matrix_path else None),
            matrix_format=(matrix_fmt or "html"),
        )
        for key, merged in _iter_merged_vectors(
            dataset_ctx,
            apply_postprocess=apply_postprocess,
        ):
            collector.update(key, merged)

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            collector.print_report(sort_key=sort)
        if not quiet:
            report_text = buffer.getvalue()
            if report_text.strip():
                print(report_text, end="")

    _run_inspect_job(
        project,
        visuals=visuals,
        log_level=log_level,
        label="Inspect report",
        section="report",
        work=_work,
    )


def partitions(
    project: str,
    *,
    output: str | None = None,
    visuals: str | None = None,
    log_level: int | None = None,
    workspace=None,
) -> None:
    """Discover observed partitions and write a manifest JSON.

    Produces a JSON with keys:
      - features: list of base feature ids
      - partitions: list of full partition ids (e.g., feature__suffix)
      - by_feature: mapping base id -> list of suffixes (empty when none)
    """

    _prepare_inspect_build(
        project,
        visuals=visuals,
        workspace=workspace,
    )

    def _work(dataset_ctx):
        project_path = dataset_ctx.project

        feature_cfgs = list(dataset_ctx.dataset.features or [])
        expected_feature_ids = [cfg.id for cfg in feature_cfgs]

        base_artifacts = artifacts_root(project_path)
        output_path = Path(output) if output else (base_artifacts / "partitions.json")

        collector = VectorStatsCollector(
            expected_feature_ids or None,
            match_partition="full",
            threshold=None,
            show_matrix=False,
        )

        for key, merged in _iter_merged_vectors(
            dataset_ctx,
            apply_postprocess=True,
        ):
            collector.update(key, merged)

        ensure_parent(output_path)

        parts = sorted(collector.discovered_partitions)
        features = sorted({pid.split("__", 1)[0] for pid in parts})
        by_feature: dict[str, list[str]] = {}
        for pid in parts:
            if "__" in pid:
                base, suffix = pid.split("__", 1)
            else:
                base, suffix = pid, ""
            by_feature.setdefault(base, [])
            if suffix and suffix not in by_feature[base]:
                by_feature[base].append(suffix)
        for k in list(by_feature.keys()):
            by_feature[k] = sorted(by_feature[k])

        data = {
            "features": features,
            "partitions": parts,
            "by_feature": by_feature,
        }

        with output_path.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
        print(f"[write] Saved partitions manifest to {output_path}")

    _run_inspect_job(
        project,
        visuals=visuals,
        log_level=log_level,
        label="Inspect partitions",
        section="partitions",
        work=_work,
    )
