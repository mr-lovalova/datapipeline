from __future__ import annotations

import json
import io
from collections import defaultdict
from contextlib import redirect_stdout
from pathlib import Path
from typing import Iterable

from datapipeline.streams.canonical import open_canonical_stream
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.pipeline.pipelines import build_feature_pipeline, build_vector_pipeline
from datapipeline.services.bootstrap import bootstrap
from datapipeline.analysis.vector_analyzer import VectorStatsCollector
from datapipeline.utils.paths import default_build_path, ensure_parent


def partitions(
    project: str,
    output: str | None,
    limit: int | None = None,
    include_targets: bool = False,
) -> None:
    """Discover partitioned feature ids and write them to a manifest."""

    project_path = Path(project)
    dataset = load_dataset(project_path, "features")
    bootstrap(project_path)

    group_by = dataset.group_by
    manifest: dict[str, list[str]] = defaultdict(list)
    all_features: set[str] = set()

    def _collect(configs: Iterable):
        for cfg in configs:
            base_id = cfg.feature_id
            seen_for_feature: set[str] = set()
            stream = build_feature_pipeline(cfg, group_by, open_canonical_stream)
            count = 0
            for feature_record in stream:
                fid = feature_record.feature_id
                if fid not in all_features:
                    all_features.add(fid)
                if fid not in seen_for_feature:
                    manifest[base_id].append(fid)
                    seen_for_feature.add(fid)
                count += 1
                if limit is not None and count >= limit:
                    break

    _collect(dataset.features or [])
    if include_targets:
        _collect(dataset.targets or [])

    payload = {
        "features": sorted(all_features),
        "by_feature": {key: sorted(values) for key, values in manifest.items()},
    }

    recipe_dir = project_path.parent
    output_path = Path(output) if output else default_build_path("partitions.json", recipe_dir)
    ensure_parent(output_path)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)

    print(f"📦 wrote partition manifest to {output_path}")


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
        None,
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

    below_features = [
        stat["id"] for stat in feature_stats if stat["coverage"] < threshold
    ]
    keep_features = [
        stat["id"] for stat in feature_stats if stat["coverage"] >= threshold
    ]

    below_partitions = [
        stat["id"] for stat in partition_stats if stat["coverage"] < threshold
    ]
    keep_partitions = [
        stat["id"] for stat in partition_stats if stat["coverage"] >= threshold
    ]
    below_suffixes = [
        pid.split("__", 1)[1] if "__" in pid else pid for pid in below_partitions
    ]
    keep_suffixes = [
        pid.split("__", 1)[1]
        for pid in keep_partitions
        if "__" in pid
    ]

    trimmed = {
        "total_vectors": summary.get("total_vectors", collector.total_vectors),
        "empty_vectors": summary.get("empty_vectors", collector.empty_vectors),
        "threshold": threshold,
        "match_partition": match_partition,
        "features": {
            "keep": keep_features,
            "below": below_features,
            "coverage": {stat["id"]: stat["coverage"] for stat in feature_stats},
        },
        "partitions": {
            "keep": keep_partitions,
            "below": below_partitions,
            "keep_suffixes": keep_suffixes,
            "below_suffixes": below_suffixes,
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
        None,
    ):
        collector.update(group_key, vector.values)

    buffer = io.StringIO()
    with redirect_stdout(buffer):
        collector.print_report()
    report = buffer.getvalue()
    if report.strip():
        print(report, end="")

    print(f"📝 Saved availability matrix to {output_path}")
