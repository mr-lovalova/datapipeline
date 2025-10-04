from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from pathlib import Path

from datapipeline.analysis.vector_analyzer import VectorStatsCollector
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.pipeline.pipelines import build_vector_pipeline
from datapipeline.services.bootstrap import bootstrap
from datapipeline.streams.canonical import open_canonical_stream
from datapipeline.utils.paths import default_build_path, ensure_parent


def report(
    project: str,
    *,
    output: str | None = None,
    threshold: float = 0.95,
    match_partition: str = "base",
    matrix: str = "none",  # one of: none|csv|html
    matrix_output: str | None = None,
    rows: int = 20,
    cols: int = 10,
    fmt: str | None = None,
    quiet: bool = False,
    write_coverage: bool = True,
    apply_vector_transforms: bool = True,
) -> None:
    """Compute a quality report and optionally export coverage JSON and/or a matrix.

    - Always prints a human-readable report (unless quiet=True).
    - When output is set, writes trimmed coverage summary JSON.
    - When matrix != 'none', writes an availability matrix in the requested format.
    """

    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    bootstrap(project_path)

    expected_feature_ids = [cfg.id for cfg in (dataset.features or [])]

    # Resolve matrix format and path
    matrix_fmt = (fmt or matrix) if matrix in {"csv", "html"} else None
    if matrix_fmt:
        filename = "matrix.html" if matrix_fmt == "html" else "matrix.csv"
    else:
        filename = None
    recipe_dir = project_path.parent
    matrix_path = None
    if matrix_fmt:
        matrix_path = Path(matrix_output) if matrix_output else default_build_path(filename, recipe_dir)

    collector = VectorStatsCollector(
        expected_feature_ids or None,
        match_partition=match_partition,
        threshold=threshold,
        show_matrix=False,
        matrix_rows=rows,
        matrix_cols=cols,
        matrix_output=(str(matrix_path) if matrix_path else None),
        matrix_format=(matrix_fmt or "csv"),
    )

    transforms = dataset.vector_transforms if apply_vector_transforms else None
    for group_key, vector in build_vector_pipeline(
        dataset.features,
        dataset.group_by,
        open_canonical_stream,
        transforms,
    ):
        collector.update(group_key, vector.values)

    buffer = io.StringIO()
    with redirect_stdout(buffer):
        summary = collector.print_report()
    if not quiet:
        report_text = buffer.getvalue()
        if report_text.strip():
            print(report_text, end="")

    # Optionally write coverage summary JSON to a path
    if write_coverage:
        output_path = Path(output) if output else default_build_path("coverage.json", recipe_dir)
        ensure_parent(output_path)

        feature_stats = summary.get("feature_stats", [])
        partition_stats = summary.get("partition_stats", [])

        trimmed = {
            "total_vectors": summary.get("total_vectors", collector.total_vectors),
            "empty_vectors": summary.get("empty_vectors", collector.empty_vectors),
            "threshold": threshold,
            "match_partition": match_partition,
            "features": {
                "keep": summary.get("keep_features", []),
                "below": summary.get("below_features", []),
                "coverage": {stat["id"]: stat["coverage"] for stat in feature_stats},
            },
            "partitions": {
                "keep": summary.get("keep_partitions", []),
                "below": summary.get("below_partitions", []),
                "keep_suffixes": summary.get("keep_suffixes", []),
                "below_suffixes": summary.get("below_suffixes", []),
                "coverage": {stat["id"]: stat["coverage"] for stat in partition_stats},
            },
        }

        with output_path.open("w", encoding="utf-8") as fh:
            json.dump(trimmed, fh, indent=2)
        print(f"📝 Saved coverage summary to {output_path}")


def partitions(
    project: str,
    *,
    output: str | None = None,
) -> None:
    """Discover observed partitions and write a manifest JSON.

    Produces a JSON with keys:
      - features: list of base feature ids
      - partitions: list of full partition ids (e.g., feature__suffix)
      - by_feature: mapping base id -> list of suffixes (empty when none)
    """

    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    bootstrap(project_path)

    expected_feature_ids = [cfg.id for cfg in (dataset.features or [])]
    collector = VectorStatsCollector(
        expected_feature_ids or None,
        match_partition="full",
        threshold=None,
        show_matrix=False,
    )

    for group_key, vector in build_vector_pipeline(
        dataset.features,
        dataset.group_by,
        open_canonical_stream,
        None,
    ):
        collector.update(group_key, vector.values)

    recipe_dir = project_path.parent
    output_path = Path(output) if output else default_build_path("partitions.json", recipe_dir)
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
    print(f"📝 Saved partitions manifest to {output_path}")
