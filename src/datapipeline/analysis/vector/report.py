from typing import Any, Literal, TYPE_CHECKING

from .matrix import export_matrix_data, render_matrix

if TYPE_CHECKING:
    from .collector import VectorStatsCollector

ReportSortKey = Literal["missing", "nulls"]


def _emit(message: str, *args: Any) -> None:
    print(message % args if args else message)


def print_report(
    collector: "VectorStatsCollector",
    *,
    sort_key: ReportSortKey = "missing",
) -> dict[str, Any]:
    summary = build_report_summary(collector, sort_key=sort_key)
    _emit_report(collector, summary, sort_key)
    _render_report_matrices(collector, summary)
    if collector.matrix_output:
        export_matrix_data(collector)
    _emit_cadence_report(summary)
    _emit_threshold_report(collector, summary)
    return summary


def build_report_summary(
    collector: "VectorStatsCollector",
    *,
    sort_key: ReportSortKey = "missing",
) -> dict[str, Any]:
    tracked_features = _tracked_features(collector)
    tracked_partitions = _tracked_partitions(collector)

    summary: dict[str, Any] = {
        "total_vectors": collector.total_vectors,
        "empty_vectors": collector.empty_vectors,
        "match_partition": collector.match_partition,
        "tracked_features": sorted(tracked_features),
        "tracked_partitions": sorted(tracked_partitions),
        "threshold": collector.threshold,
    }

    if not collector.total_vectors:
        summary.update(
            {
                "feature_stats": [],
                "partition_stats": [],
                "below_features": [],
                "keep_features": [],
                "below_partitions": [],
                "keep_partitions": [],
                "below_suffixes": [],
                "keep_suffixes": [],
            }
        )
        return summary

    feature_stats = _feature_stats(collector, tracked_features, sort_key)
    partition_stats = _partition_stats(collector, tracked_partitions)
    summary["feature_stats"] = feature_stats
    summary["partition_stats"] = partition_stats
    summary.update(_threshold_summary(collector, feature_stats, partition_stats))
    return summary


def _tracked_features(collector: "VectorStatsCollector") -> set[str]:
    if collector.expected_features:
        return collector.expected_features
    return collector.discovered_features


def _tracked_partitions(collector: "VectorStatsCollector") -> set[str]:
    if collector.match_partition == "full" and collector.expected_features:
        return set(collector.expected_features)
    return collector.discovered_partitions


def _feature_stats(
    collector: "VectorStatsCollector",
    tracked_features: set[str],
    sort_key: ReportSortKey,
) -> list[dict[str, Any]]:
    def feature_sort(feature_id: str) -> int:
        if sort_key == "nulls":
            return collector._feature_null_count(feature_id)
        return collector._coverage(feature_id)[1]

    stats: list[dict[str, Any]] = []
    for feature_id in sorted(tracked_features, key=feature_sort, reverse=True):
        present, missing, opportunities = collector._coverage(feature_id)
        coverage = present / opportunities if opportunities else 0.0
        nulls = collector._feature_null_count(feature_id)
        cadence_nulls = collector.cadence_null_counts.get(feature_id, 0)
        cadence_opps = collector.cadence_opportunities.get(feature_id, 0)
        raw_samples = collector.missing_samples.get(feature_id, [])
        stats.append(
            {
                "id": feature_id,
                "present": present,
                "missing": missing,
                "nulls": nulls,
                "cadence_nulls": cadence_nulls,
                "cadence_opportunities": cadence_opps,
                "cadence_null_fraction": (
                    cadence_nulls / cadence_opps if cadence_opps else None
                ),
                "coverage": coverage,
                "opportunities": opportunities,
                "samples": [
                    {
                        "group": collector._format_group_key(group_key),
                        "status": status,
                    }
                    for group_key, status in raw_samples
                ],
            }
        )
    return stats


def _partition_stats(
    collector: "VectorStatsCollector",
    tracked_partitions: set[str],
) -> list[dict[str, Any]]:
    stats: list[dict[str, Any]] = []
    for partition_id in tracked_partitions:
        present, missing, opportunities = collector._coverage(
            partition_id, partitions=True
        )
        coverage = present / opportunities if opportunities else 0.0
        nulls = collector.null_counts_partitions.get(partition_id, 0)
        cadence_nulls = collector.cadence_null_counts_partitions.get(partition_id, 0)
        cadence_opps = collector.cadence_opportunities_partitions.get(partition_id, 0)
        raw_samples = collector.missing_partition_samples.get(partition_id, [])
        stats.append(
            {
                "id": partition_id,
                "base": _base_partition(partition_id),
                "present": present,
                "missing": missing,
                "nulls": nulls,
                "cadence_nulls": cadence_nulls,
                "cadence_opportunities": cadence_opps,
                "cadence_null_fraction": (
                    cadence_nulls / cadence_opps if cadence_opps else None
                ),
                "coverage": coverage,
                "opportunities": opportunities,
                "samples": [
                    {
                        "group": collector._format_group_key(group_key),
                        "status": status,
                    }
                    for group_key, status in raw_samples
                ],
            }
        )
    return stats


def _threshold_summary(
    collector: "VectorStatsCollector",
    feature_stats: list[dict[str, Any]],
    partition_stats: list[dict[str, Any]],
) -> dict[str, Any]:
    below_features: list[str] = []
    above_features: list[str] = []
    below_partitions: list[str] = []
    above_partitions: list[str] = []
    below_suffixes: list[str] = []
    above_suffixes: list[str] = []
    below_partition_values: list[str] = []
    above_partition_values: list[str] = []
    below_partitions_cadence: list[str] = []
    above_partitions_cadence: list[str] = []

    if collector.threshold is not None:
        threshold = collector.threshold
        below_features = [
            stats["id"] for stats in feature_stats if stats["coverage"] < threshold
        ]
        above_features = [
            stats["id"] for stats in feature_stats if stats["coverage"] >= threshold
        ]
        if partition_stats:
            below_partitions = [
                stats["id"] for stats in partition_stats
                if stats["coverage"] < threshold
            ]
            above_partitions = [
                stats["id"] for stats in partition_stats
                if stats["coverage"] >= threshold
            ]
            below_suffixes = [
                collector._partition_suffix(pid) for pid in below_partitions
            ]
            above_suffixes = [
                collector._partition_suffix(pid)
                for pid in above_partitions
                if collector._partition_suffix(pid) != pid
            ]
            if not above_partitions:
                above_suffixes = []
            below_partition_values = [
                value
                for pid in below_partitions
                if "__" in pid and (value := collector._partition_value(pid))
            ]
            above_partition_values = [
                value
                for pid in above_partitions
                if "__" in pid and (value := collector._partition_value(pid))
            ]
            below_partitions_cadence = [
                stats["id"]
                for stats in partition_stats
                if (stats.get("cadence_null_fraction") or 0) > (1 - threshold)
            ]
            above_partitions_cadence = [
                stats["id"]
                for stats in partition_stats
                if (stats.get("cadence_null_fraction") or 0) <= (1 - threshold)
            ]

    return {
        "below_features": below_features,
        "keep_features": above_features or [stats["id"] for stats in feature_stats],
        "below_partitions": below_partitions,
        "keep_partitions": above_partitions
        or [stats["id"] for stats in partition_stats],
        "below_suffixes": below_suffixes,
        "keep_suffixes": above_suffixes
        or (
            [
                collector._partition_suffix(stats["id"])
                for stats in partition_stats
                if collector._partition_suffix(stats["id"]) != stats["id"]
            ]
            if partition_stats
            else []
        ),
        "below_partition_values": below_partition_values,
        "keep_partition_values": above_partition_values
        or (
            [
                collector._partition_value(stats["id"])
                for stats in partition_stats
                if "__" in stats["id"] and collector._partition_value(stats["id"])
            ]
            if partition_stats
            else []
        ),
        "below_partitions_cadence": below_partitions_cadence,
        "keep_partitions_cadence": above_partitions_cadence
        or [stats["id"] for stats in partition_stats],
    }


def _emit_report(
    collector: "VectorStatsCollector",
    summary: dict[str, Any],
    sort_key: ReportSortKey,
) -> None:
    tracked_features = set(summary["tracked_features"])
    _emit_header(collector, summary)
    if not summary["total_vectors"]:
        _emit("(no vectors analyzed)")
        return

    feature_stats = summary["feature_stats"]
    partition_stats = summary["partition_stats"]
    _emit_feature_stats(collector, feature_stats, sort_key)
    _emit_partition_stats(partition_stats, sort_key)
    if collector.show_matrix:
        _emit_matrix_gap_details(collector, tracked_features, partition_stats)


def _emit_header(
    collector: "VectorStatsCollector",
    summary: dict[str, Any],
) -> None:
    _emit("\n=== Vector Quality Report ===")
    _emit("Total vectors processed: %d", summary["total_vectors"])
    _emit("Empty vectors: %d", summary["empty_vectors"])
    _emit(
        "Features tracked (%s): %d",
        summary["match_partition"],
        len(summary["tracked_features"]),
    )
    if collector.match_partition == "full":
        _emit("Partitions observed: %d", len(collector.discovered_partitions))


def _emit_feature_stats(
    collector: "VectorStatsCollector",
    feature_stats: list[dict[str, Any]],
    sort_key: ReportSortKey,
) -> None:
    sort_label = "null" if sort_key == "nulls" else "missing"
    _emit("\n-> Feature coverage (sorted by %s count):", sort_label)
    for stats in feature_stats:
        line = (
            f"  - {stats['id']}: present {stats['present']}/{stats['opportunities']}"
            f" ({stats['coverage']:.1%}) | absent {stats['missing']} | null {stats['nulls']}"
        )
        sample_note = collector._format_samples(
            collector.missing_samples.get(stats["id"], [])
        )
        if sample_note:
            line += f"; samples: {sample_note}"
        _emit(line)


def _emit_partition_stats(
    partition_stats: list[dict[str, Any]],
    sort_key: ReportSortKey,
) -> None:
    if not partition_stats:
        return
    sort_label = "null" if sort_key == "nulls" else "absent"
    _emit("\n-> Partition details (top by %s count):", sort_label)

    def partition_sort(stats: dict[str, Any]) -> int:
        return stats["nulls"] if sort_key == "nulls" else stats["missing"]

    for stats in sorted(partition_stats, key=partition_sort, reverse=True)[:20]:
        line = (
            f"  - {stats['id']} (base: {stats['base']}): present {stats['present']}/{stats['opportunities']}"
            f" ({stats['coverage']:.1%}) | absent {stats['missing']} | null/invalid {stats['nulls']}"
        )
        _emit(line)


def _render_report_matrices(
    collector: "VectorStatsCollector",
    summary: dict[str, Any],
) -> None:
    if not collector.show_matrix:
        return

    feature_stats = summary["feature_stats"]
    partition_stats = summary["partition_stats"]
    feature_candidates = (
        summary["below_features"]
        or [stats["id"] for stats in feature_stats if stats["missing"] > 0]
        or [stats["id"] for stats in feature_stats]
    )
    selected_features = _limit_matrix_columns(collector, feature_candidates)
    if selected_features:
        render_matrix(collector, features=selected_features)

    if not partition_stats:
        return

    partition_candidates = (
        summary["below_partitions"]
        or [stats["id"] for stats in partition_stats if stats["missing"] > 0]
        or [stats["id"] for stats in partition_stats]
    )
    selected_partitions = _limit_matrix_columns(collector, partition_candidates)
    if selected_partitions:
        render_matrix(collector, features=selected_partitions, partitions=True)


def _limit_matrix_columns(
    collector: "VectorStatsCollector",
    candidates: list[str],
) -> list[str]:
    if collector.matrix_cols is None:
        return candidates
    return candidates[: collector.matrix_cols]


def _emit_matrix_gap_details(
    collector: "VectorStatsCollector",
    tracked_features: set[str],
    partition_stats: list[dict[str, Any]],
) -> None:
    group_missing = [
        (
            group,
            sum(
                1
                for feature_id in tracked_features
                if collector.group_feature_status[group].get(feature_id, "absent")
                != "present"
            ),
        )
        for group in collector.group_feature_status
    ]
    group_missing = [item for item in group_missing if item[1] > 0]
    if group_missing:
        _emit("\n-> Time buckets with missing features:")
        for group, count in sorted(
            group_missing, key=lambda item: item[1], reverse=True
        )[:10]:
            _emit(
                "  - %s: %d features missing",
                collector._format_group_key(group),
                count,
            )

    if not partition_stats:
        return

    partition_missing = [
        (
            group,
            sum(
                1
                for partition_id in collector.group_partition_status[group]
                if collector.group_partition_status[group].get(
                    partition_id, "absent"
                )
                != "present"
            ),
        )
        for group in collector.group_partition_status
    ]
    partition_missing = [item for item in partition_missing if item[1] > 0]
    if partition_missing:
        _emit("\n-> Time buckets with missing partitions:")
        for group, count in sorted(
            partition_missing, key=lambda item: item[1], reverse=True
        )[:10]:
            _emit(
                "  - %s: %d partitions missing",
                collector._format_group_key(group),
                count,
            )


def _emit_cadence_report(summary: dict[str, Any]) -> None:
    partition_cadence = [
        stats
        for stats in summary["partition_stats"]
        if stats.get("cadence_opportunities")
    ]
    if not partition_cadence:
        return

    _emit("\n-> Record-level gaps (expected cadence; null/invalid elements):")
    total_missing = sum(s.get("cadence_nulls", 0) or 0 for s in partition_cadence)
    total_opps = sum(
        s.get("cadence_opportunities", 0) or 0 for s in partition_cadence
    )
    if total_opps:
        _emit(
            "  Total null/invalid elements: %d/%d (%.1f%%)",
            total_missing,
            total_opps,
            (total_missing / total_opps) * 100,
        )
    _emit("  Top partitions by null/invalid elements:")
    for stats in sorted(
        partition_cadence,
        key=lambda item: (item.get("cadence_nulls") or 0),
        reverse=True,
    )[:20]:
        missing_elems = stats.get("cadence_nulls") or 0
        opps = stats.get("cadence_opportunities") or 0
        fraction = (missing_elems / opps) if opps else 0
        _emit(
            "  - %s (base: %s): vectors present %d/%d | absent %d | cadence null/invalid %d/%d elements (%.1f%%)",
            stats["id"],
            stats.get("base"),
            stats.get("present", 0),
            stats.get("opportunities", 0),
            stats.get("missing", 0),
            missing_elems,
            opps,
            fraction * 100,
        )


def _emit_threshold_report(
    collector: "VectorStatsCollector",
    summary: dict[str, Any],
) -> None:
    threshold = summary["threshold"]
    if threshold is None:
        return

    _emit(
        "\n[low] Features below %.0f%% coverage:\n  below_features = %s",
        threshold * 100,
        summary["below_features"],
    )
    _emit(
        "[high] Features at/above %.0f%% coverage:\n  keep_features = %s",
        threshold * 100,
        [
            item["id"]
            for item in summary["feature_stats"]
            if item["coverage"] >= threshold
        ],
    )
    if not summary["partition_stats"]:
        return

    above_partitions = [
        item["id"]
        for item in summary["partition_stats"]
        if item["coverage"] >= threshold
    ]
    above_suffixes = [
        collector._partition_suffix(partition_id)
        for partition_id in above_partitions
        if collector._partition_suffix(partition_id) != partition_id
    ]
    above_partition_values = [
        value
        for partition_id in above_partitions
        if "__" in partition_id and (value := collector._partition_value(partition_id))
    ]

    _emit(
        "\n[low] Partitions below %.0f%% coverage:\n  below_partitions = %s",
        threshold * 100,
        summary["below_partitions"],
    )
    _emit("  below_suffixes = %s", summary["below_suffixes"])
    if summary["below_partition_values"]:
        _emit("  below_partition_values = %s", summary["below_partition_values"])
    _emit(
        "[high] Partitions at/above %.0f%% coverage:\n  keep_partitions = %s",
        threshold * 100,
        above_partitions,
    )
    _emit("  keep_suffixes = %s", above_suffixes)
    if above_partition_values:
        _emit("  keep_partition_values = %s", above_partition_values)
    if summary["below_partitions_cadence"]:
        _emit(
            "[low] Partitions below %.0f%% cadence fill:\n  below_partitions_cadence = %s",
            threshold * 100,
            summary["below_partitions_cadence"],
        )
    if summary["keep_partitions_cadence"]:
        _emit(
            "[high] Partitions at/above %.0f%% cadence fill:\n  keep_partitions_cadence = %s",
            threshold * 100,
            summary["keep_partitions_cadence"],
        )


def _base_partition(partition_id: str) -> str:
    return partition_id.split("__", 1)[0] if "__" in partition_id else partition_id
