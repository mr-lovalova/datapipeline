from pathlib import Path
from typing import Any, Mapping, Optional
import logging

from datapipeline.analysis.vector.collector import VectorStatsCollector
from datapipeline.analysis.vector.snapshot import collector_from_snapshot
from datapipeline.analysis.vector.matrix import export_matrix_data
from datapipeline.cli.visuals.execution import emit_execution_message
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.metadata import build_vector_metadata_lookup
from datapipeline.config.tasks import OperationTask
from datapipeline.dag.context import PipelineContext
from datapipeline.runtime import Runtime
from datapipeline.services.artifacts import VECTOR_METADATA_SPEC, VECTOR_STATS_SPEC
from datapipeline.utils.paths import ensure_parent

logger = logging.getLogger(__name__)

def _option(options: Mapping[str, Any], key: str, default: Any) -> Any:
    value = options.get(key, default)
    return default if value is None else value


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


def _load_collector(
    runtime: Runtime,
    *,
    options: Mapping[str, Any],
    matrix_format: str | None = None,
    matrix_path: Path | None = None,
) -> VectorStatsCollector:
    context = PipelineContext(runtime)
    snapshot = context.require_artifact(VECTOR_STATS_SPEC)
    if not isinstance(snapshot, dict):
        raise RuntimeError("Invalid vector stats artifact; expected a JSON object.")
    expected_feature_ids, schema_meta = build_vector_metadata_lookup(
        context.require_artifact(VECTOR_METADATA_SPEC)
    )
    return collector_from_snapshot(
        snapshot,
        expected_feature_ids=expected_feature_ids,
        schema_meta=schema_meta,
        threshold=float(_option(options, "threshold", 0.95)),
        show_matrix=False,
        matrix_rows=int(_option(options, "rows", 20)),
        matrix_cols=int(_option(options, "cols", 10)),
        matrix_output=(str(matrix_path) if matrix_path is not None else None),
        matrix_format=(matrix_format or "html"),
    )


def _build_summary(
    collector: VectorStatsCollector,
    *,
    sort_key: str,
    threshold: float,
) -> dict[str, Any]:
    tracked_features = (
        set(collector.expected_features)
        if collector.expected_features
        else set(collector.discovered_features)
    )
    tracked_partitions = set(collector.discovered_partitions)

    feature_stats: list[dict[str, Any]] = []
    for feature_id in tracked_features:
        present, missing, opportunities = collector._coverage(feature_id)
        coverage = present / opportunities if opportunities else 0.0
        feature_stats.append(
            {
                "id": feature_id,
                "present": present,
                "missing": missing,
                "nulls": collector._feature_null_count(feature_id),
                "coverage": coverage,
                "opportunities": opportunities,
            }
        )
    feature_stats.sort(
        key=(lambda item: item["nulls"] if sort_key == "nulls" else item["missing"]),
        reverse=True,
    )

    partition_stats: list[dict[str, Any]] = []
    for partition_id in tracked_partitions:
        present, missing, opportunities = collector._coverage(
            partition_id, partitions=True
        )
        coverage = present / opportunities if opportunities else 0.0
        cadence_nulls = collector.cadence_null_counts_partitions.get(partition_id, 0)
        cadence_opportunities = collector.cadence_opportunities_partitions.get(
            partition_id, 0
        )
        partition_stats.append(
            {
                "id": partition_id,
                "base": collector._normalize(partition_id),
                "present": present,
                "missing": missing,
                "nulls": collector.null_counts_partitions.get(partition_id, 0),
                "coverage": coverage,
                "opportunities": opportunities,
                "cadence_nulls": cadence_nulls,
                "cadence_opportunities": cadence_opportunities,
                "cadence_null_fraction": (
                    cadence_nulls / cadence_opportunities
                    if cadence_opportunities
                    else None
                ),
            }
        )
    partition_stats.sort(
        key=(lambda item: item["nulls"] if sort_key == "nulls" else item["missing"]),
        reverse=True,
    )

    below_features = [
        item["id"] for item in feature_stats if item["coverage"] < threshold
    ]
    keep_features = [
        item["id"] for item in feature_stats if item["coverage"] >= threshold
    ] or [item["id"] for item in feature_stats]

    below_partitions = [
        item["id"] for item in partition_stats if item["coverage"] < threshold
    ]
    keep_partitions = [
        item["id"] for item in partition_stats if item["coverage"] >= threshold
    ] or [item["id"] for item in partition_stats]

    below_suffixes = [collector._partition_suffix(pid) for pid in below_partitions]
    keep_suffixes = [
        collector._partition_suffix(pid)
        for pid in keep_partitions
        if collector._partition_suffix(pid) != pid
    ]

    below_partition_values = [
        collector._partition_value(pid)
        for pid in below_partitions
        if "__" in pid and collector._partition_value(pid)
    ]
    keep_partition_values = [
        collector._partition_value(pid)
        for pid in keep_partitions
        if "__" in pid and collector._partition_value(pid)
    ]

    keep_partitions_cadence = [
        item["id"]
        for item in partition_stats
        if (item.get("cadence_null_fraction") or 0.0) <= (1 - threshold)
    ] or [item["id"] for item in partition_stats]

    return {
        "total_vectors": collector.total_vectors,
        "empty_vectors": collector.empty_vectors,
        "tracked_features": sorted(tracked_features),
        "feature_stats": feature_stats,
        "partition_stats": partition_stats,
        "below_features": below_features,
        "keep_features": keep_features,
        "below_partitions": below_partitions,
        "keep_partitions": keep_partitions,
        "below_suffixes": below_suffixes,
        "keep_suffixes": keep_suffixes,
        "below_partition_values": below_partition_values,
        "keep_partition_values": keep_partition_values,
        "keep_partitions_cadence": keep_partitions_cadence,
    }


def _resolve_summary(
    runtime: Runtime,
    *,
    options: Mapping[str, Any],
) -> tuple[dict[str, Any], str, float]:
    sort_key = str(_option(options, "sort", "missing"))
    threshold = float(_option(options, "threshold", 0.95))
    collector = _load_collector(runtime, options=options)
    summary = _build_summary(collector, sort_key=sort_key, threshold=threshold)
    return summary, sort_key, threshold


def _print_coverage(summary: Mapping[str, Any], *, sort_key: str) -> None:
    features = list(summary.get("feature_stats") or [])
    partitions = list(summary.get("partition_stats") or [])
    metric = "null" if sort_key == "nulls" else "missing"

    print("\n=== Vector Coverage ===")
    print(f"Total vectors processed: {summary.get('total_vectors', 0)}")
    print(f"Empty vectors: {summary.get('empty_vectors', 0)}")
    print(f"Features tracked: {len(summary.get('tracked_features') or [])}")
    print(f"\n-> Feature coverage (sorted by {metric}):")
    for stats in features:
        print(
            f"  - {stats['id']}: present {stats['present']}/{stats['opportunities']} "
            f"({stats['coverage']:.1%}) | absent {stats['missing']} | null {stats['nulls']}"
        )
    if partitions:
        print(f"\n-> Partition coverage (sorted by {metric}):")
        for stats in partitions:
            print(
                f"  - {stats['id']} (base: {stats['base']}): present {stats['present']}/{stats['opportunities']} "
                f"({stats['coverage']:.1%}) | absent {stats['missing']} | null {stats['nulls']}"
            )


def _print_thresholds(summary: Mapping[str, Any], *, threshold: float) -> None:
    pct = threshold * 100
    print("\n=== Vector Thresholds ===")
    print(f"Threshold: {pct:.0f}%")
    print(f"\n[low] Features below {pct:.0f}% coverage:")
    print(f"  below_features = {summary.get('below_features', [])}")
    print(f"[high] Features at/above {pct:.0f}% coverage:")
    print(f"  keep_features = {summary.get('keep_features', [])}")
    print(f"\n[low] Partitions below {pct:.0f}% coverage:")
    print(f"  below_partitions = {summary.get('below_partitions', [])}")
    print(f"  below_suffixes = {summary.get('below_suffixes', [])}")
    values = summary.get("below_partition_values", [])
    if values:
        print(f"  below_partition_values = {values}")
    print(f"[high] Partitions at/above {pct:.0f}% coverage:")
    print(f"  keep_partitions = {summary.get('keep_partitions', [])}")
    print(f"  keep_suffixes = {summary.get('keep_suffixes', [])}")
    keep_values = summary.get("keep_partition_values", [])
    if keep_values:
        print(f"  keep_partition_values = {keep_values}")
    print(f"[high] Partitions at/above {pct:.0f}% cadence fill:")
    print(f"  keep_partitions_cadence = {summary.get('keep_partitions_cadence', [])}")


def inspect_coverage_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int] = None,
    target=None,
    throttle_ms: Optional[float] = None,
    stage: Optional[int] = None,
    visuals: Optional[str] = None,
    operation_task: OperationTask | None = None,
) -> None:
    _ = dataset, limit, target, throttle_ms, stage, visuals
    options = operation_task.options if operation_task is not None else {}
    summary, sort_key, _ = _resolve_summary(runtime, options=options)
    if not bool(_option(options, "quiet", False)):
        _print_coverage(summary, sort_key=sort_key)


def inspect_thresholds_with_runtime(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
    limit: Optional[int] = None,
    target=None,
    throttle_ms: Optional[float] = None,
    stage: Optional[int] = None,
    visuals: Optional[str] = None,
    operation_task: OperationTask | None = None,
) -> None:
    _ = dataset, limit, target, throttle_ms, stage, visuals
    options = operation_task.options if operation_task is not None else {}
    summary, _, threshold = _resolve_summary(runtime, options=options)
    if not bool(_option(options, "quiet", False)):
        _print_thresholds(summary, threshold=threshold)


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
    _ = dataset, limit, throttle_ms, stage, visuals
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
    collector = _load_collector(
        runtime,
        options=options,
        matrix_format=matrix_format,
        matrix_path=matrix_path,
    )
    written = export_matrix_data(collector)
    if written is not None:
        emit_execution_message(
            f"Materialized inspect_matrix: {written} (format={matrix_format})",
            level=logging.INFO,
            logger=logger,
            message_kind="materialized",
        )
