from collections import Counter, defaultdict
from typing import Any, Hashable

from .collector import VectorStatsCollector


def _group_key(value: Hashable) -> str:
    return VectorStatsCollector._format_group_key(value)


def _serialize_group_map(
    values: dict[Hashable, dict[str, str]] | dict[Hashable, dict[str, list[str]]],
) -> dict[str, dict[str, str] | dict[str, list[str]]]:
    return {_group_key(group): dict(statuses) for group, statuses in values.items()}


def _serialize_samples(
    values: defaultdict[str, list[tuple[Hashable, str]]],
) -> dict[str, list[list[str]]]:
    return {
        identifier: [[_group_key(group), status] for group, status in samples]
        for identifier, samples in values.items()
    }


def _deserialize_group_map(values: dict[str, Any]) -> defaultdict[str, dict]:
    return defaultdict(dict, {group: dict(statuses) for group, statuses in values.items()})


def _deserialize_samples(values: dict[str, list[list[str]]]) -> defaultdict[str, list[tuple[str, str]]]:
    return defaultdict(
        list,
        {
            identifier: [(group, status) for group, status in samples]
            for identifier, samples in values.items()
        },
    )


def snapshot_from_collector(collector: VectorStatsCollector) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "match_partition": collector.match_partition,
        "expected_features": sorted(collector.expected_features),
        "schema_meta": collector.schema_meta,
        "sample_limit": collector.sample_limit,
        "total_vectors": collector.total_vectors,
        "empty_vectors": collector.empty_vectors,
        "discovered_features": sorted(collector.discovered_features),
        "discovered_partitions": sorted(collector.discovered_partitions),
        "seen_counts": dict(collector.seen_counts),
        "null_counts_features": dict(collector.null_counts_features),
        "seen_counts_partitions": dict(collector.seen_counts_partitions),
        "null_counts_partitions": dict(collector.null_counts_partitions),
        "cadence_null_counts": dict(collector.cadence_null_counts),
        "cadence_opportunities": dict(collector.cadence_opportunities),
        "cadence_null_counts_partitions": dict(collector.cadence_null_counts_partitions),
        "cadence_opportunities_partitions": dict(collector.cadence_opportunities_partitions),
        "missing_samples": _serialize_samples(collector.missing_samples),
        "missing_partition_samples": _serialize_samples(collector.missing_partition_samples),
        "group_feature_status": _serialize_group_map(collector.group_feature_status),
        "group_partition_status": _serialize_group_map(collector.group_partition_status),
        "group_feature_sub": _serialize_group_map(collector.group_feature_sub),
        "group_partition_sub": _serialize_group_map(collector.group_partition_sub),
    }


def collector_from_snapshot(
    snapshot: dict[str, Any],
    *,
    threshold: float | None,
    show_matrix: bool,
    matrix_rows: int,
    matrix_cols: int,
    matrix_output: str | None,
    matrix_format: str,
) -> VectorStatsCollector:
    collector = VectorStatsCollector(
        expected_feature_ids=snapshot.get("expected_features"),
        match_partition=snapshot.get("match_partition", "base"),
        schema_meta=snapshot.get("schema_meta"),
        sample_limit=snapshot.get("sample_limit", 5),
        threshold=threshold,
        show_matrix=show_matrix,
        matrix_rows=matrix_rows,
        matrix_cols=matrix_cols,
        matrix_output=matrix_output,
        matrix_format=matrix_format,
    )

    collector.total_vectors = snapshot.get("total_vectors", 0)
    collector.empty_vectors = snapshot.get("empty_vectors", 0)
    collector.discovered_features = set(snapshot.get("discovered_features", []))
    collector.discovered_partitions = set(snapshot.get("discovered_partitions", []))

    collector.seen_counts = Counter(snapshot.get("seen_counts", {}))
    collector.null_counts_features = Counter(snapshot.get("null_counts_features", {}))
    collector.seen_counts_partitions = Counter(snapshot.get("seen_counts_partitions", {}))
    collector.null_counts_partitions = Counter(snapshot.get("null_counts_partitions", {}))
    collector.cadence_null_counts = Counter(snapshot.get("cadence_null_counts", {}))
    collector.cadence_opportunities = Counter(snapshot.get("cadence_opportunities", {}))
    collector.cadence_null_counts_partitions = Counter(
        snapshot.get("cadence_null_counts_partitions", {})
    )
    collector.cadence_opportunities_partitions = Counter(
        snapshot.get("cadence_opportunities_partitions", {})
    )

    collector.missing_samples = _deserialize_samples(snapshot.get("missing_samples", {}))
    collector.missing_partition_samples = _deserialize_samples(
        snapshot.get("missing_partition_samples", {})
    )
    collector.group_feature_status = _deserialize_group_map(
        snapshot.get("group_feature_status", {})
    )
    collector.group_partition_status = _deserialize_group_map(
        snapshot.get("group_partition_status", {})
    )
    collector.group_feature_sub = _deserialize_group_map(
        snapshot.get("group_feature_sub", {})
    )
    collector.group_partition_sub = _deserialize_group_map(
        snapshot.get("group_partition_sub", {})
    )
    return collector

