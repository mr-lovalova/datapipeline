from __future__ import annotations
from typing import Any

from .matrix import export_matrix_data, render_matrix
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .collector import VectorStatsCollector


def print_report(collector: VectorStatsCollector) -> dict[str, Any]:
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

    print("\n=== Vector Quality Report ===")
    print(f"Total vectors processed: {collector.total_vectors}")
    print(f"Empty vectors: {collector.empty_vectors}")
    print(
        f"Features tracked ({collector.match_partition}): {len(tracked_features)}"
    )
    if collector.match_partition == "full":
        print(f"Partitions observed: {len(collector.discovered_partitions)}")

    if not collector.total_vectors:
        print("(no vectors analyzed)")
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
    print("\n-> Feature coverage (sorted by missing count):")
    for feature_id in sorted(
        tracked_features,
        key=lambda fid: collector._coverage(fid)[1],
        reverse=True,
    ):
        present, missing, opportunities = collector._coverage(feature_id)
        coverage = present / opportunities if opportunities else 0.0
        nulls = collector._feature_null_count(feature_id)
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
            f" ({coverage:.1%}) | missing {missing} | null {nulls}"
        )
        if sample_note:
            line += f"; samples: {sample_note}"
        print(line)
        feature_stats.append(
            {
                "id": feature_id,
                "present": present,
                "missing": missing,
                "nulls": nulls,
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
            raw_samples = collector.missing_partition_samples.get(
                partition_id, [])
            partition_stats.append(
                {
                    "id": partition_id,
                    "base": _base_partition(partition_id),
                    "present": present,
                    "missing": missing,
                    "nulls": nulls,
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

        print("\n-> Partition details (top by missing count):")
        for stats in sorted(
            partition_stats, key=lambda s: s["missing"], reverse=True
        )[:20]:
            line = (
                f"  - {stats['id']} (base: {stats['base']}): present {stats['present']}/{stats['opportunities']}"
                f" ({stats['coverage']:.1%}) | missing {stats['missing']} | null/invalid {stats['nulls']}"
            )
            print(line)

    summary["partition_stats"] = partition_stats

    below_features: list[str] = []
    above_features: list[str] = []
    below_partitions: list[str] = []
    above_partitions: list[str] = []
    below_suffixes: list[str] = []
    above_suffixes: list[str] = []

    if collector.threshold is not None:
        thr = collector.threshold
        below_features = [
            stats["id"] for stats in feature_stats if stats["coverage"] < thr
        ]
        above_features = [
            stats["id"] for stats in feature_stats if stats["coverage"] >= thr
        ]
        print(
            f"\n[low] Features below {thr:.0%} coverage:\n  below_features = {below_features}"
        )
        print(
            f"[high] Features at/above {thr:.0%} coverage:\n  keep_features = {above_features}"
        )

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
            print(
                f"\n[low] Partitions below {thr:.0%} coverage:\n  below_partitions = {below_partitions}"
            )
            print(f"  below_suffixes = {below_suffixes}")
            print(
                f"[high] Partitions at/above {thr:.0%} coverage:\n  keep_partitions = {above_partitions}"
            )
            print(f"  keep_suffixes = {above_suffixes}")

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
            print("\n-> Time buckets with missing features:")
            for group, count in sorted(
                group_missing, key=lambda item: item[1], reverse=True
            )[:10]:
                print(
                    f"  - {collector._format_group_key(group)}: {count} features missing"
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
                print("\n-> Time buckets with missing partitions:")
                for group, count in sorted(
                    partition_missing, key=lambda item: item[1], reverse=True
                )[:10]:
                    print(
                        f"  - {collector._format_group_key(group)}: {count} partitions missing"
                    )

    if collector.matrix_output:
        export_matrix_data(collector)

    return summary


def _base_partition(partition_id: str) -> str:
    return partition_id.split("__", 1)[0] if "__" in partition_id else partition_id
