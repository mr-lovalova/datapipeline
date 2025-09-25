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


def coverage(
    project: str,
    output: str | None,
    threshold: float = 0.95,
    match_partition: str = "base",
) -> None:
    """Compute coverage summary and write it to JSON."""

    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    bootstrap(project_path)

    expected_feature_ids = [cfg.feature_id for cfg in (dataset.features or [])]
    collector = VectorStatsCollector(
        expected_feature_ids or None,
        match_partition=match_partition,
        threshold=threshold,
    )

    for group_key, vector in build_vector_pipeline(
        dataset.features,
        dataset.group_by,
        open_canonical_stream,
        dataset.vector_transforms,
    ):
        collector.update(group_key, vector.values)

    buffer = io.StringIO()
    with redirect_stdout(buffer):
        summary = collector.print_report()
    report = buffer.getvalue()
    if report.strip():
        print(report, end="")

    recipe_dir = project_path.parent
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


def matrix(
    project: str,
    output: str | None,
    threshold: float = 0.95,
    rows: int = 20,
    cols: int = 10,
    fmt: str = "csv",
) -> None:
    """Export availability matrix to CSV or HTML."""

    project_path = Path(project)
    dataset = load_dataset(project_path, "vectors")
    bootstrap(project_path)

    expected_feature_ids = [cfg.feature_id for cfg in (dataset.features or [])]
    filename = "matrix.html" if fmt == "html" else "matrix.csv"
    recipe_dir = project_path.parent
    output_path = Path(output) if output else default_build_path(filename, recipe_dir)

    collector = VectorStatsCollector(
        expected_feature_ids or None,
        threshold=threshold,
        show_matrix=False,
        matrix_rows=rows,
        matrix_cols=cols,
        matrix_output=str(output_path),
        matrix_format=fmt,
    )

    for group_key, vector in build_vector_pipeline(
        dataset.features,
        dataset.group_by,
        open_canonical_stream,
        dataset.vector_transforms,
    ):
        collector.update(group_key, vector.values)

    buffer = io.StringIO()
    with redirect_stdout(buffer):
        collector.print_report()
    report = buffer.getvalue()
    if report.strip():
        print(report, end="")

    print(f"📝 Saved availability matrix to {output_path}")
