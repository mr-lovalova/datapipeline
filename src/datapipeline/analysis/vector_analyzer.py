import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Hashable, Iterable, Literal


def _base_feature_id(feature_id: str) -> str:
    """Return the base feature id without partition suffix."""

    if "__" in feature_id:
        return feature_id.split("__", 1)[0]
    return feature_id


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        return value != value  # NaN without numpy
    return False


class VectorStatsCollector:
    """Collect coverage statistics for feature vectors."""

    def __init__(
        self,
        expected_feature_ids: Iterable[str] | None = None,
        *,
        match_partition: Literal["base", "full"] = "base",
        sample_limit: int = 5,
        threshold: float | None = 0.95,
        show_matrix: bool = False,
        matrix_rows: int = 20,
        matrix_cols: int = 10,
        matrix_output: str | None = None,
        matrix_format: str = "csv",
    ) -> None:
        self.match_partition = match_partition
        self.threshold = threshold
        self.show_matrix = show_matrix
        self.matrix_rows = matrix_rows if matrix_rows and matrix_rows > 0 else None
        self.matrix_cols = matrix_cols if matrix_cols and matrix_cols > 0 else None
        self.matrix_output = Path(matrix_output) if matrix_output else None
        self.matrix_format = matrix_format

        self.expected_features = (
            {self._normalize(fid) for fid in expected_feature_ids}
            if expected_feature_ids
            else set()
        )

        self.discovered_features: set[str] = set()
        self.discovered_partitions: set[str] = set()

        self.total_vectors = 0
        self.empty_vectors = 0

        self.present_counts = Counter()
        self.present_counts_partitions = Counter()
        self.null_counts_partitions = Counter()

        self.missing_samples = defaultdict(list)
        self.missing_partition_samples = defaultdict(list)
        self.sample_limit = sample_limit

        self.group_feature_status = defaultdict(dict)
        self.group_partition_status = defaultdict(dict)

    def _normalize(self, feature_id: str) -> str:
        if self.match_partition == "full":
            return feature_id
        return _base_feature_id(feature_id)

    def update(self, group_key: Hashable, feature_vector: dict[str, Any]) -> None:
        self.total_vectors += 1

        present_partitions = set(feature_vector.keys())
        if not present_partitions:
            self.empty_vectors += 1

        status_features = self.group_feature_status[group_key]
        status_partitions = self.group_partition_status[group_key]

        present_normalized: set[str] = set()
        seen_partitions: set[str] = set()
        for partition_id in present_partitions:
            normalized = self._normalize(partition_id)
            present_normalized.add(normalized)
            seen_partitions.add(partition_id)

            value = feature_vector[partition_id]

            status_features.setdefault(normalized, "present")
            status_partitions.setdefault(partition_id, "present")

            self.discovered_features.add(normalized)
            self.discovered_partitions.add(partition_id)

            is_null = _is_missing_value(value)
            if is_null:
                status_features[normalized] = "null"
                status_partitions[partition_id] = "null"
                self.null_counts_partitions[partition_id] += 1
                if len(self.missing_partition_samples[partition_id]) < self.sample_limit:
                    self.missing_partition_samples[partition_id].append(
                        (group_key, "null")
                    )
                if len(self.missing_samples[normalized]) < self.sample_limit:
                    self.missing_samples[normalized].append(
                        (group_key, "null"))

        for normalized in present_normalized:
            if status_features.get(normalized) == "present":
                self.present_counts[normalized] += 1

        for partition_id in seen_partitions:
            if status_partitions.get(partition_id) == "present":
                self.present_counts_partitions[partition_id] += 1

        tracked_features = (
            self.expected_features if self.expected_features else self.discovered_features
        )
        missing_features = tracked_features - present_normalized
        for feature_id in missing_features:
            if status_features.get(feature_id) != "null":
                status_features[feature_id] = "absent"
            if len(self.missing_samples[feature_id]) < self.sample_limit:
                self.missing_samples[feature_id].append((group_key, "absent"))

        if self.match_partition == "full":
            tracked_partitions = (
                set(self.expected_features) if self.expected_features else self.discovered_partitions
            )
        else:
            tracked_partitions = self.discovered_partitions

        missing_partitions = tracked_partitions - present_partitions
        for partition_id in missing_partitions:
            if status_partitions.get(partition_id) != "null":
                status_partitions[partition_id] = "absent"
            if len(self.missing_partition_samples[partition_id]) < self.sample_limit:
                self.missing_partition_samples[partition_id].append(
                    (group_key, "absent")
                )

    def _coverage(
        self, identifier: str, *, partitions: bool = False
    ) -> tuple[int, int, int]:
        present = (
            self.present_counts_partitions[identifier]
            if partitions
            else self.present_counts[identifier]
        )
        opportunities = self.total_vectors
        missing = max(opportunities - present, 0)
        return present, missing, opportunities

    def _feature_null_count(self, feature_id: str) -> int:
        total = 0
        for partition_id, count in self.null_counts_partitions.items():
            if self._normalize(partition_id) == feature_id:
                total += count
        return total

    @staticmethod
    def _format_group_key(group_key: Hashable) -> str:
        if isinstance(group_key, tuple):
            return ", ".join(str(part) for part in group_key)
        return str(group_key)

    @staticmethod
    def _symbol_for(status: str) -> str:
        return {
            "present": "#",
            "null": "!",
            "absent": ".",
        }.get(status, ".")

    @staticmethod
    def _format_samples(samples: list[tuple[Hashable, str]], limit: int = 3) -> str:
        if not samples:
            return ""
        trimmed = samples[:limit]
        rendered = ", ".join(
            f"{reason}@{sample}" for sample, reason in trimmed)
        if len(samples) > limit:
            rendered += ", …"
        return rendered

    @staticmethod
    def _partition_suffix(partition_id: str) -> str:
        return partition_id.split("__", 1)[1] if "__" in partition_id else partition_id

    def _render_matrix(
        self,
        *,
        features: list[str],
        partitions: bool = False,
        column_width: int = 6,
    ) -> None:
        status_map = self.group_partition_status if partitions else self.group_feature_status
        if not status_map or not features:
            return

        column_width = max(column_width, min(
            10, max(len(fid) for fid in features)))

        def status_for(group: Hashable, fid: str) -> str:
            statuses = status_map.get(group, {})
            return statuses.get(fid, "absent")

        sorted_groups = sorted(status_map.keys(), key=lambda g: str(g))
        focus_groups = [
            g
            for g in sorted_groups
            if any(status_for(g, fid) != "present" for fid in features)
        ]
        if not focus_groups:
            focus_groups = sorted_groups
        if self.matrix_rows is not None:
            focus_groups = focus_groups[: self.matrix_rows]

        matrix_label = "Partition" if partitions else "Feature"
        print(f"\n→ {matrix_label} availability heatmap:")

        header = " " * 20 + " ".join(
            f"{fid[-column_width:]:>{column_width}}" for fid in features
        )
        print(header)

        for group in focus_groups:
            label = self._format_group_key(group)
            label = label[:18].ljust(18)
            cells = " ".join(
                f"{self._symbol_for(status_for(group, fid)):^{column_width}}"
                for fid in features
            )
            print(f"  {label} {cells}")

        print("    Legend: # present | ! null | . missing")

    def print_report(self) -> None:
        tracked_features = (
            self.expected_features if self.expected_features else self.discovered_features
        )
        tracked_partitions = (
            set(self.expected_features)
            if self.match_partition == "full" and self.expected_features
            else self.discovered_partitions
        )

        summary: dict[str, Any] = {
            "total_vectors": self.total_vectors,
            "empty_vectors": self.empty_vectors,
            "match_partition": self.match_partition,
            "tracked_features": sorted(tracked_features),
            "tracked_partitions": sorted(tracked_partitions),
            "threshold": self.threshold,
        }

        print("\n=== Vector Quality Report ===")
        print(f"Total vectors processed: {self.total_vectors}")
        print(f"Empty vectors: {self.empty_vectors}")
        print(
            f"Features tracked ({self.match_partition}): {len(tracked_features)}"
        )
        if self.match_partition == "full":
            print(f"Partitions observed: {len(self.discovered_partitions)}")

        if not self.total_vectors:
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
        print("\n→ Feature coverage (sorted by missing count):")
        for feature_id in sorted(
            tracked_features,
            key=lambda fid: self._coverage(fid)[1],
            reverse=True,
        ):
            present, missing, opportunities = self._coverage(feature_id)
            coverage = present / opportunities if opportunities else 0.0
            nulls = self._feature_null_count(feature_id)
            raw_samples = self.missing_samples.get(feature_id, [])
            sample_note = self._format_samples(raw_samples)
            samples = [
                {
                    "group": self._format_group_key(group_key),
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
                present, missing, opportunities = self._coverage(
                    partition_id, partitions=True
                )
                coverage = present / opportunities if opportunities else 0.0
                nulls = self.null_counts_partitions.get(partition_id, 0)
                raw_samples = self.missing_partition_samples.get(
                    partition_id, [])
                partition_stats.append(
                    {
                        "id": partition_id,
                        "base": _base_feature_id(partition_id),
                        "present": present,
                        "missing": missing,
                        "nulls": nulls,
                        "coverage": coverage,
                        "opportunities": opportunities,
                        "samples": [
                            {
                                "group": self._format_group_key(group_key),
                                "status": status,
                            }
                            for group_key, status in raw_samples
                        ],
                    }
                )

            print("\n→ Partition details (top by missing count):")
            for stats in sorted(partition_stats, key=lambda s: s["missing"], reverse=True)[
                :20
            ]:
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

        if self.threshold is not None:
            thr = self.threshold
            below_features = [
                stats["id"] for stats in feature_stats if stats["coverage"] < thr
            ]
            above_features = [
                stats["id"] for stats in feature_stats if stats["coverage"] >= thr
            ]
            print(
                f"\n📉 Features below {thr:.0%} coverage:\n  below_features = {below_features}"
            )
            print(
                f"📈 Features at/above {thr:.0%} coverage:\n  keep_features = {above_features}"
            )

            if partition_stats:
                below_partitions = [
                    stats["id"]
                    for stats in partition_stats
                    if stats["coverage"] < thr
                ]
                above_partitions = [
                    stats["id"]
                    for stats in partition_stats
                    if stats["coverage"] >= thr
                ]
                below_suffixes = [
                    self._partition_suffix(pid)
                    for pid in below_partitions
                ]
                above_suffixes = [
                    self._partition_suffix(pid)
                    for pid in above_partitions
                    if self._partition_suffix(pid) != pid
                ]
                if not above_partitions:
                    above_suffixes = []
                print(
                    f"\n📉 Partitions below {thr:.0%} coverage:\n  below_partitions = {below_partitions}"
                )
                print(f"  below_suffixes = {below_suffixes}")
                print(
                    f"📈 Partitions at/above {thr:.0%} coverage:\n  keep_partitions = {above_partitions}"
                )
                print(f"  keep_suffixes = {above_suffixes}")

            summary.update(
                {
                    "below_features": below_features,
                    "keep_features": above_features,
                    "below_partitions": below_partitions,
                    "keep_partitions": above_partitions,
                    "below_suffixes": below_suffixes,
                    "keep_suffixes": above_suffixes,
                }
            )
        else:
            summary.update(
                {
                    "below_features": [],
                    "keep_features": [stats["id"] for stats in feature_stats],
                    "below_partitions": [],
                    "keep_partitions": [stats["id"] for stats in partition_stats],
                    "below_suffixes": [],
                    "keep_suffixes": [
                        self._partition_suffix(stats["id"])
                        for stats in partition_stats
                        if self._partition_suffix(stats["id"]) != stats["id"]
                    ]
                    if partition_stats
                    else [],
                }
            )

        if self.show_matrix:
            feature_candidates = (
                below_features
                or [stats["id"] for stats in feature_stats if stats["missing"] > 0]
                or [stats["id"] for stats in feature_stats]
            )
            selected_features = (
                feature_candidates
                if self.matrix_cols is None
                else feature_candidates[: self.matrix_cols]
            )
            if selected_features:
                self._render_matrix(features=selected_features)

            if partition_stats:
                partition_candidates = (
                    below_partitions
                    or [stats["id"] for stats in partition_stats if stats["missing"] > 0]
                    or [stats["id"] for stats in partition_stats]
                )
                selected_partitions = (
                    partition_candidates
                    if self.matrix_cols is None
                    else partition_candidates[: self.matrix_cols]
                )
                if selected_partitions:
                    self._render_matrix(
                        features=selected_partitions, partitions=True)

            group_missing = [
                (
                    group,
                    sum(
                        1
                        for fid in tracked_features
                        if self.group_feature_status[group].get(fid, "absent") != "present"
                    ),
                )
                for group in self.group_feature_status
            ]
            group_missing = [item for item in group_missing if item[1] > 0]
            if group_missing:
                print("\n→ Time buckets with missing features:")
                for group, count in sorted(group_missing, key=lambda item: item[1], reverse=True)[:10]:
                    print(
                        f"  - {self._format_group_key(group)}: {count} features missing")

            if partition_stats:
                partition_missing = [
                    (
                        group,
                        sum(
                            1
                            for pid in self.group_partition_status[group]
                            if self.group_partition_status[group].get(pid, "absent") != "present"
                        ),
                    )
                    for group in self.group_partition_status
                ]
                partition_missing = [
                    item for item in partition_missing if item[1] > 0]
                if partition_missing:
                    print("\n→ Time buckets with missing partitions:")
                    for group, count in sorted(
                        partition_missing, key=lambda item: item[1], reverse=True
                    )[:10]:
                        print(
                            f"  - {self._format_group_key(group)}: {count} partitions missing")

        if self.matrix_output:
            self._export_matrix_data()

        return summary

    def _export_matrix_data(self) -> None:
        if not self.matrix_output:
            return

        path = self.matrix_output
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            if self.matrix_format == "html":
                self._write_matrix_html(path)
            else:
                self._write_matrix_csv(path)
            print(f"\n📝 Saved availability matrix to {path}")
        except OSError as exc:
            print(f"\n⚠️ Failed to write availability matrix to {path}: {exc}")

    def _collect_feature_ids(self) -> list[str]:
        feature_ids: set[str] = set()
        for statuses in self.group_feature_status.values():
            feature_ids.update(statuses.keys())
        return sorted(feature_ids)

    def _collect_partition_ids(self) -> list[str]:
        partition_ids: set[str] = set()
        for statuses in self.group_partition_status.values():
            partition_ids.update(statuses.keys())
        return sorted(partition_ids)

    def _collect_group_keys(self) -> list[Hashable]:
        keys = set(self.group_feature_status.keys()) | set(
            self.group_partition_status.keys()
        )
        return sorted(keys, key=lambda g: str(g))

    def _write_matrix_csv(self, path: Path) -> None:
        rows: list[tuple[str, str, str, str]] = []
        for group, statuses in self.group_feature_status.items():
            group_key = self._format_group_key(group)
            for fid, status in statuses.items():
                rows.append(("feature", fid, group_key, status))

        for group, statuses in self.group_partition_status.items():
            group_key = self._format_group_key(group)
            for pid, status in statuses.items():
                rows.append(("partition", pid, group_key, status))

        with path.open("w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["kind", "identifier", "group_key", "status"])
            writer.writerows(rows)

    def _write_matrix_html(self, path: Path) -> None:
        feature_ids = self._collect_feature_ids()
        partition_ids = self._collect_partition_ids()
        group_keys = self._collect_group_keys()

        def render_table(title: str, identifiers: list[str], status_map: dict) -> str:
            if not identifiers:
                return f"<h2>{title}</h2><p>No data.</p>"

            cell_class = {
                "present": "status-present",
                "null": "status-null",
                "absent": "status-absent",
            }

            header = "".join(
                f"<th>{id_}</th>" for id_ in identifiers
            )
            rows_html = []
            for group in group_keys:
                key_str = self._format_group_key(group)
                statuses = status_map.get(group, {})
                cells = []
                for identifier in identifiers:
                    status = statuses.get(identifier, "absent")
                    cls = cell_class.get(status, "status-absent")
                    symbol = self._symbol_for(status)
                    cells.append(
                        f"<td class='{cls}' title='{status}'>{symbol}</td>")
                rows_html.append(
                    f"<tr><th>{key_str}</th>{''.join(cells)}</tr>"
                )

            return (
                f"<h2>{title}</h2>"
                "<table class='heatmap'>"
                f"<tr><th>Group</th>{header}</tr>"
                f"{''.join(rows_html)}"
                "</table>"
            )

        feature_table = render_table(
            "Feature Availability",
            feature_ids,
            self.group_feature_status,
        )
        partition_table = render_table(
            "Partition Availability",
            partition_ids,
            self.group_partition_status,
        )

        style = """
            body { font-family: Arial, sans-serif; }
            table.heatmap { border-collapse: collapse; margin-bottom: 2rem; }
            .heatmap th, .heatmap td { border: 1px solid #ccc; padding: 4px 6px; }
            .heatmap th { background: #f0f0f0; position: sticky; top: 0; }
            .status-present { background: #2ecc71; color: #fff; }
            .status-null { background: #f1c40f; color: #000; }
            .status-absent { background: #e74c3c; color: #fff; }
        """

        html = (
            "<html><head><meta charset='utf-8'>"
            f"<style>{style}</style>"
            "<title>Feature Availability</title></head><body>"
            f"<h1>Availability Matrix</h1>{feature_table}{partition_table}"
            "</body></html>"
        )

        with path.open("w", encoding="utf-8") as fh:
            fh.write(html)
