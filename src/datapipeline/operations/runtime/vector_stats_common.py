from typing import Any, Mapping

from datapipeline.analysis.vector.collector import VectorStatsCollector
from datapipeline.analysis.vector.snapshot import collector_from_snapshot
from datapipeline.config.metadata import build_vector_metadata_lookup
from datapipeline.config.tasks import OperationTask
from datapipeline.dag.context import PipelineContext
from datapipeline.operations.persistence import RuntimeOutput
from datapipeline.runtime import Runtime
from datapipeline.services.artifacts import VECTOR_METADATA_SPEC, VECTOR_STATS_SPEC


def option(options: Mapping[str, Any], key: str, default: Any) -> Any:
    value = options.get(key, default)
    return default if value is None else value


def options_for_task(operation_task: OperationTask | None) -> Mapping[str, Any]:
    if operation_task is None:
        return {}
    return operation_task.options


def load_collector(
    runtime: Runtime,
    *,
    options: Mapping[str, Any],
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
        threshold=float(option(options, "threshold", 0.95)),
        show_matrix=False,
        matrix_rows=int(option(options, "rows", 20)),
        matrix_cols=int(option(options, "cols", 10)),
        matrix_output=None,
        matrix_format="html",
    )


def build_metrics(
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


def resolve_metrics(
    runtime: Runtime,
    *,
    options: Mapping[str, Any],
) -> tuple[dict[str, Any], str, float]:
    sort_key = str(option(options, "sort", "missing"))
    threshold = float(option(options, "threshold", 0.95))
    collector = load_collector(runtime, options=options)
    metrics = build_metrics(collector, sort_key=sort_key, threshold=threshold)
    return metrics, sort_key, threshold


def report_payload(
    report: str,
    metrics: Mapping[str, Any],
    *,
    sort_key: str | None = None,
    threshold: float | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "report": report,
        "metrics": dict(metrics),
    }
    if sort_key is not None:
        payload["sort"] = sort_key
    if threshold is not None:
        payload["threshold"] = threshold
    return payload


def write_summary_output(
    payload: Mapping[str, Any],
) -> RuntimeOutput:
    return RuntimeOutput(
        payload=payload,
    )


def matrix_status_rows(
    collector: VectorStatsCollector,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for group, statuses in collector.group_feature_status.items():
        group_key = collector._format_group_key(group)
        for identifier, status in statuses.items():
            rows.append(
                {
                    "matrix_kind": "feature",
                    "identifier": identifier,
                    "group_key": group_key,
                    "status": status,
                }
            )
    for group, statuses in collector.group_partition_status.items():
        group_key = collector._format_group_key(group)
        for identifier, status in statuses.items():
            rows.append(
                {
                    "matrix_kind": "partition",
                    "identifier": identifier,
                    "group_key": group_key,
                    "status": status,
                }
            )
    rows.sort(
        key=lambda row: (
            row["matrix_kind"],
            row["identifier"],
            row["group_key"],
            row["status"],
        )
    )
    return rows
