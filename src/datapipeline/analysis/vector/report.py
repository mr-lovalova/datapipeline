from __future__ import annotations
from typing import Any, Literal, TYPE_CHECKING
import logging

from .matrix import export_matrix_data, render_matrix

if TYPE_CHECKING:
    from .collector import VectorStatsCollector


logger = logging.getLogger(__name__)


def print_report(
    collector: VectorStatsCollector,
    *,
    sort_key: Literal["missing", "nulls"] = "missing",
) -> dict[str, Any]:
    tracked_features = (
        collector.expected_features
        if collector.expected_features
        else collector.discovered_features
    )
    tracked_partitions = (
        set(collector.expected_features)
        if collector.match_partition == "full" and collector.expected_features
        else collector.discovered_partitions
    )

    summary: dict[str, Any] = {
        "total_vectors": collector.total_vectors,
        "empty_vectors": collector.empty_vectors,
        "match_partition": collector.match_partition,
        "tracked_features": sorted(tracked_features),
        "tracked_partitions": sorted(tracked_partitions),
        "threshold": collector.threshold,
    }

    logger.info("\n=== Vector Quality Report ===")
    logger.info("Total vectors processed: %d", collector.total_vectors)
    logger.info("Empty vectors: %d", collector.empty_vectors)
    logger.info(
        "Features tracked (%s): %d",
        collector.match_partition,
        len(tracked_features),
    )
    if collector.match_partition == "full":
        logger.info("Partitions observed: %d",
                    len(collector.discovered_partitions))

    if not collector.total_vectors:
        logger.info("(no vectors analyzed)")
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

    feature_stats = []
    sort_label = "null" if sort_key == "nulls" else "missing"
    logger.info("\n-> Feature coverage (sorted by %s count):", sort_label)
    if sort_key == "nulls":
        def _feature_sort(fid):
            return collector._feature_null_count(fid)
    else:
        def _feature_sort(fid):
            return collector._coverage(fid)[1]

    for feature_id in sorted(
        tracked_features,
        key=_feature_sort,
        reverse=True,
    ):
        present, missing, opportunities = collector._coverage(feature_id)
        coverage = present / opportunities if opportunities else 0.0
        nulls = collector._feature_null_count(feature_id)
        cadence_nulls = collector.cadence_null_counts.get(feature_id, 0)
        cadence_opps = collector.cadence_opportunities.get(feature_id, 0)
        raw_samples = collector.missing_samples.get(feature_id, [])
        sample_note = collector._format_samples(raw_samples)
        samples = [
            {
                "group": collector._format_group_key(group_key),
                "status": status,
            }
            for group_key, status in raw_samples
        ]
        line = (
            f"  - {feature_id}: present {present}/{opportunities}"
            f" ({coverage:.1%}) | absent {missing} | null {nulls}"
        )
        if sample_note:
            line += f"; samples: {sample_note}"
        logger.info(line)
        feature_stats.append(
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
                "samples": samples,
            }
        )

    summary["feature_stats"] = feature_stats

    partition_stats = []
    if tracked_partitions:
        for partition_id in tracked_partitions:
            present, missing, opportunities = collector._coverage(
                partition_id, partitions=True
            )
            coverage = present / opportunities if opportunities else 0.0
            nulls = collector.null_counts_partitions.get(partition_id, 0)
            cadence_nulls = collector.cadence_null_counts_partitions.get(
                partition_id, 0
            )
            cadence_opps = collector.cadence_opportunities_partitions.get(
                partition_id, 0
            )
            raw_samples = collector.missing_partition_samples.get(
                partition_id, [])
            partition_stats.append(
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

        sort_label_partitions = "null" if sort_key == "nulls" else "absent"
        logger.info("\n-> Partition details (top by %s count):", sort_label_partitions)
        def _partition_sort(stats):
            return stats["nulls"] if sort_key == "nulls" else stats["missing"]
        for stats in sorted(
            partition_stats, key=_partition_sort, reverse=True
        )[:20]:
            line = (
                f"  - {stats['id']} (base: {stats['base']}): present {stats['present']}/{stats['opportunities']}"
                f" ({stats['coverage']:.1%}) | absent {stats['missing']} | null/invalid {stats['nulls']}"
            )
            logger.info(line)

    summary["partition_stats"] = partition_stats

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
        thr = collector.threshold
        below_features = [
            stats["id"] for stats in feature_stats if stats["coverage"] < thr
        ]
        above_features = [
            stats["id"] for stats in feature_stats if stats["coverage"] >= thr
        ]
        if partition_stats:
            below_partitions = [
                stats["id"] for stats in partition_stats if stats["coverage"] < thr
            ]
            above_partitions = [
                stats["id"] for stats in partition_stats if stats["coverage"] >= thr
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
                v
                for pid in below_partitions
                if "__" in pid and (v := collector._partition_value(pid))
            ]
            above_partition_values = [
                v
                for pid in above_partitions
                if "__" in pid and (v := collector._partition_value(pid))
            ]
            below_partitions_cadence = [
                stats["id"]
                for stats in partition_stats
                if (stats.get("cadence_null_fraction") or 0) > (1 - thr)
            ]
            above_partitions_cadence = [
                stats["id"]
                for stats in partition_stats
                if (stats.get("cadence_null_fraction") or 0) <= (1 - thr)
            ]

    summary.update(
        {
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
                    if "__" in stats["id"]
                    and collector._partition_value(stats["id"])
                ]
                if partition_stats
                else []
            ),
            "below_partitions_cadence": below_partitions_cadence,
            "keep_partitions_cadence": above_partitions_cadence
            or [stats["id"] for stats in partition_stats],
        }
    )

    if collector.show_matrix:
        feature_candidates = (
            below_features
            or [
                stats["id"]
                for stats in feature_stats
                if stats["missing"] > 0
            ]
            or [stats["id"] for stats in feature_stats]
        )
        selected_features = (
            feature_candidates
            if collector.matrix_cols is None
            else feature_candidates[: collector.matrix_cols]
        )
        if selected_features:
            render_matrix(collector, features=selected_features)

        if partition_stats:
            partition_candidates = (
                below_partitions
                or [
                    stats["id"]
                    for stats in partition_stats
                    if stats["missing"] > 0
                ]
                or [stats["id"] for stats in partition_stats]
            )
            selected_partitions = (
                partition_candidates
                if collector.matrix_cols is None
                else partition_candidates[: collector.matrix_cols]
            )
            if selected_partitions:
                render_matrix(
                    collector, features=selected_partitions, partitions=True
                )

        group_missing = [
            (
                group,
                sum(
                    1
                    for fid in tracked_features
                    if collector.group_feature_status[group].get(fid, "absent")
                    != "present"
                ),
            )
            for group in collector.group_feature_status
        ]
        group_missing = [item for item in group_missing if item[1] > 0]
        if group_missing:
            logger.info("\n-> Time buckets with missing features:")
            for group, count in sorted(
                group_missing, key=lambda item: item[1], reverse=True
            )[:10]:
                logger.info(
                    "  - %s: %d features missing",
                    collector._format_group_key(group),
                    count,
                )

        if partition_stats:
            partition_missing = [
                (
                    group,
                    sum(
                        1
                        for pid in collector.group_partition_status[group]
                        if collector.group_partition_status[group].get(pid, "absent")
                        != "present"
                    ),
                )
                for group in collector.group_partition_status
            ]
            partition_missing = [
                item for item in partition_missing if item[1] > 0]
            if partition_missing:
                logger.info("\n-> Time buckets with missing partitions:")
                for group, count in sorted(
                    partition_missing, key=lambda item: item[1], reverse=True
                )[:10]:
                    logger.info(
                        "  - %s: %d partitions missing",
                        collector._format_group_key(group),
                        count,
                    )

    if collector.matrix_output:
        export_matrix_data(collector)

    # Record-level (cadence) gaps for list features/partitions
    partition_cadence = [
        stats
        for stats in partition_stats
        if stats.get("cadence_opportunities")
    ]
    if partition_cadence:
        logger.info("\n-> Record-level gaps (expected cadence; null/invalid elements):")
        total_missing = sum(s.get("cadence_nulls", 0) or 0 for s in partition_cadence)
        total_opps = sum(s.get("cadence_opportunities", 0) or 0 for s in partition_cadence)
        if total_opps:
            logger.info(
                "  Total null/invalid elements: %d/%d (%.1f%%)",
                total_missing,
                total_opps,
                (total_missing / total_opps) * 100,
            )
        logger.info("  Top partitions by null/invalid elements:")
        for stats in sorted(
            partition_cadence,
            key=lambda s: (s.get("cadence_nulls") or 0),
            reverse=True,
        )[:20]:
            missing_elems = stats.get("cadence_nulls") or 0
            opps = stats.get("cadence_opportunities") or 0
            frac = (missing_elems / opps) if opps else 0
            logger.info(
                "  - %s (base: %s): vectors present %d/%d | absent %d | cadence null/invalid %d/%d elements (%.1f%%)",
                stats["id"],
                stats.get("base"),
                stats.get("present", 0),
                stats.get("opportunities", 0),
                stats.get("missing", 0),
                missing_elems,
                opps,
                frac * 100,
            )

    if collector.threshold is not None:
        thr = collector.threshold
        logger.warning(
            "\n[low] Features below %.0f%% coverage:\n  below_features = %s",
            thr * 100,
            below_features,
        )
        logger.info(
            "[high] Features at/above %.0f%% coverage:\n  keep_features = %s",
            thr * 100,
            above_features,
        )
        if partition_stats:
            logger.warning(
                "\n[low] Partitions below %.0f%% coverage:\n  below_partitions = %s",
                thr * 100,
                below_partitions,
            )
            logger.warning("  below_suffixes = %s", below_suffixes)
            if below_partition_values:
                logger.warning("  below_partition_values = %s",
                               below_partition_values)
            logger.info(
                "[high] Partitions at/above %.0f%% coverage:\n  keep_partitions = %s",
                thr * 100,
                above_partitions,
            )
            logger.info("  keep_suffixes = %s", above_suffixes)
            if above_partition_values:
                logger.info(
                    "  keep_partition_values = %s", above_partition_values)
            if below_partitions_cadence:
                logger.warning(
                    "[low] Partitions below %.0f%% cadence fill:\n  below_partitions_cadence = %s",
                    thr * 100,
                    below_partitions_cadence,
                )
            if above_partitions_cadence:
                logger.info(
                    "[high] Partitions at/above %.0f%% cadence fill:\n  keep_partitions_cadence = %s",
                    thr * 100,
                    above_partitions_cadence,
                )

    return summary


def _base_partition(partition_id: str) -> str:
    return partition_id.split("__", 1)[0] if "__" in partition_id else partition_id
