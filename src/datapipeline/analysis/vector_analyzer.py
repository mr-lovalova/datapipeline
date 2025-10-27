import base64
import csv
import html
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Hashable, Iterable, Literal
from datapipeline.transforms.vector_utils import base_id as _base_id
from datetime import datetime


def _base_feature_id(feature_id: str) -> str:
    """Return the base feature id without partition suffix."""
    return _base_id(feature_id)


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
        matrix_format: str = "html",
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
        # Optional per-cell sub-status for list-valued entries (finer resolution inside a bucket)
        self.group_feature_sub: dict[Hashable,
                                     dict[str, list[str]]] = defaultdict(dict)
        self.group_partition_sub: dict[Hashable,
                                       dict[str, list[str]]] = defaultdict(dict)

    @staticmethod
    def _group_sort_key(g: Hashable):
        """Stable, chronological sort key for group keys.

        Many pipelines use a 1-tuple containing a datetime as the group key.
        Sorting by ``str(g)`` can produce lexicographic mis-ordering (e.g.,
        hours "3" vs "21"). This helper prefers numeric datetime ordering and
        falls back to string representation only when needed.
        """
        def norm(p: Any):
            if isinstance(p, datetime):
                # Use POSIX timestamp for monotonic ordering
                return p.timestamp()
            return p

        if isinstance(g, (tuple, list)):
            return tuple(norm(p) for p in g)
        return norm(g)

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

            # Capture sub-status for list-valued entries
            sub: list[str] | None = None
            if isinstance(value, list):
                sub = []
                for v in value:
                    if v is None or (isinstance(v, float) and v != v):
                        sub.append("null")
                    else:
                        sub.append("present")
                if sub:
                    self.group_partition_sub[group_key][partition_id] = sub
                    # Only store one sub per normalized id (first seen)
                    self.group_feature_sub[group_key].setdefault(
                        normalized, sub)

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

        sorted_groups = sorted(status_map.keys(), key=self._group_sort_key)
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
        return sorted(keys, key=self._group_sort_key)

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

        sections: list[str] = []
        scripts: list[str] = []
        legend_entries: dict[str, tuple[str, str]] = {}

        def render_table(
            title: str,
            identifiers: list[str],
            status_map: dict,
            sub_map: dict,
            table_id: str,
        ) -> None:
            if not identifiers:
                sections.append(
                    "<section class='matrix-section'>"
                    f"<h2>{html.escape(title)}</h2>"
                    "<p>No data.</p>"
                    "</section>"
                )
                return

            base_class = {
                "present": "status-present",
                "null": "status-null",
                "absent": "status-absent",
            }

            statuses_found: set[str] = {"absent"}
            for statuses in status_map.values():
                statuses_found.update(statuses.values())
            for group_sub in sub_map.values():
                for sub_statuses in group_sub.values():
                    statuses_found.update(sub_statuses)

            preferred_order = {"present": 0, "null": 1, "absent": 2}
            ordered_statuses = sorted(
                statuses_found,
                key=lambda s: (preferred_order.get(s, len(preferred_order)), s),
            )
            status_to_index = {status: idx for idx, status in enumerate(ordered_statuses)}
            default_index = status_to_index["absent"]

            row_labels = [self._format_group_key(group) for group in group_keys]
            row_count = len(row_labels)
            col_count = len(identifiers)
            total_cells = row_count * col_count
            codes = bytearray(total_cells)
            sub_indices: dict[int, list[int]] = {}

            for row_idx, group in enumerate(group_keys):
                statuses = status_map.get(group, {})
                sub_cells = sub_map.get(group, {})
                for col_idx, identifier in enumerate(identifiers):
                    cell_idx = row_idx * col_count + col_idx
                    status = statuses.get(identifier, "absent")
                    codes[cell_idx] = status_to_index.get(status, default_index)
                    sub_statuses = sub_cells.get(identifier)
                    if sub_statuses:
                        sub_indices[cell_idx] = [
                            status_to_index.get(sub_status, default_index)
                            for sub_status in sub_statuses
                        ]

            status_class_map = {
                status: base_class.get(status, "status-missing")
                for status in ordered_statuses
            }
            status_class_map["__default__"] = "status-missing"

            symbol_map = {
                status: self._symbol_for(status)
                for status in ordered_statuses
            }
            symbol_map["__default__"] = "."

            payload = {
                "rows": row_labels,
                "cols": identifiers,
                "statuses": ordered_statuses,
                "encoded": base64.b64encode(bytes(codes)).decode("ascii"),
                "sub": sub_indices,
                "statusClass": status_class_map,
                "symbols": symbol_map,
            }

            header_cells = "".join(
                f"<th scope='col'>{html.escape(identifier)}</th>"
                for identifier in identifiers
            )

            sections.append(
                "<section class='matrix-section'>"
                f"<h2>{html.escape(title)}</h2>"
                "<div class='matrix-info'>Scroll horizontally and vertically to explore.</div>"
                f"<div class='table-container' id='{table_id}-container'>"
                "<table class='heatmap'>"
                "<thead>"
                "<tr>"
                "<th scope='col' class='group-col'>Group</th>"
                f"{header_cells}"
                "</tr>"
                "</thead>"
                f"<tbody id='{table_id}-body' data-colspan='{col_count + 1}'></tbody>"
                "</table>"
                "</div>"
                "</section>"
            )

            scripts.append(
                f"setupMatrix('{table_id}', {json.dumps(payload)});"
            )

            for status, css_class in status_class_map.items():
                if status.startswith("__"):
                    continue
                legend_entries.setdefault(
                    status, (css_class, status.replace("_", " ").title())
                )

        render_table(
            "Feature Availability",
            feature_ids,
            self.group_feature_status,
            self.group_feature_sub,
            "feature",
        )
        render_table(
            "Partition Availability",
            partition_ids,
            self.group_partition_status,
            self.group_partition_sub,
            "partition",
        )

        for base_status, css in (
            ("present", "status-present"),
            ("null", "status-null"),
            ("absent", "status-absent"),
        ):
            legend_entries.setdefault(
                base_status, (css, base_status.replace("_", " ").title())
            )

        ordered_legend = sorted(
            legend_entries.items(),
            key=lambda item: (
                {"present": 0, "null": 1, "absent": 2}.get(item[0], 99),
                item[0],
            ),
        )
        legend_html = "".join(
            f"<span><span class='swatch {css}'></span>{label}</span>"
            for _, (css, label) in ordered_legend
        )

        style = """
            :root { color-scheme: light; }
            * { box-sizing: border-box; }
            body {
                font-family: Arial, sans-serif;
                margin: 24px;
                background: #f9f9fa;
                color: #222;
            }
            h1 { margin: 0; }
            .matrix-wrapper {
                border: 1px solid #d0d0d0;
                border-radius: 8px;
                background: #fff;
                box-shadow: 0 1px 3px rgba(0,0,0,0.08);
                overflow: hidden;
            }
            .matrix-header {
                padding: 16px 20px;
                border-bottom: 1px solid #e2e2e8;
                display: flex;
                flex-wrap: wrap;
                align-items: center;
                gap: 16px;
            }
            .matrix-header h1 {
                font-size: 22px;
            }
            .legend {
                display: inline-flex;
                flex-wrap: wrap;
                gap: 12px;
                font-size: 13px;
                color: #444;
            }
            .legend span {
                display: inline-flex;
                align-items: center;
                gap: 8px;
            }
            .legend .swatch {
                width: 14px;
                height: 14px;
                border-radius: 3px;
                border: 1px solid rgba(0,0,0,0.08);
                background: #bdc3c7;
            }
            .matrix-section {
                padding: 18px 20px 24px;
                border-top: 1px solid #f0f0f4;
            }
            .matrix-section:first-of-type {
                border-top: none;
            }
            .matrix-section h2 {
                margin: 0 0 10px;
                font-size: 18px;
            }
            .matrix-info {
                font-size: 12px;
                color: #666;
                margin-bottom: 10px;
            }
            .table-container {
                border: 1px solid #d0d0d0;
                border-radius: 6px;
                overflow: auto;
                max-height: 55vh;
            }
            table.heatmap {
                border-collapse: collapse;
                min-width: 100%;
            }
            .heatmap th,
            .heatmap td {
                border: 1px solid #d0d0d0;
                padding: 4px 6px;
                text-align: center;
                font-size: 13px;
                line-height: 1.2;
            }
            .heatmap thead th {
                position: sticky;
                top: 0;
                background: #f3f3f6;
                z-index: 2;
            }
            .heatmap thead th.group-col,
            .heatmap tbody th.group-col {
                min-width: 160px;
            }
            .heatmap tbody th {
                position: sticky;
                left: 0;
                background: #fff;
                text-align: left;
                font-weight: normal;
                color: #333;
                z-index: 1;
            }
            .status-present { background: #2ecc71; color: #fff; font-weight: bold; }
            .status-null { background: #f1c40f; color: #000; font-weight: bold; }
            .status-absent { background: #e74c3c; color: #fff; font-weight: bold; }
            .status-missing { background: #bdc3c7; color: #000; font-weight: bold; }
            .sub { display: flex; gap: 1px; height: 12px; }
            .sub span { flex: 1; display: block; }
            .sub span::after { content: ""; display: block; width: 100%; height: 100%; }
            .sub .status-present::after { background: #2ecc71; }
            .sub .status-null::after { background: #f1c40f; }
            .sub .status-absent::after { background: #e74c3c; }
            .sub .status-missing::after { background: #bdc3c7; }
        """

        script = """
            function setupMatrix(rootId, payload) {
                const container = document.getElementById(rootId + "-container");
                const tbody = document.getElementById(rootId + "-body");
                if (!container || !tbody) {
                    return;
                }

                const rows = payload.rows;
                const cols = payload.cols;
                const statuses = payload.statuses;
                const encoded = payload.encoded;
                const sub = payload.sub || {};
                const statusClass = payload.statusClass || {};
                const symbols = payload.symbols || {};
                const colCount = cols.length;
                const totalRows = rows.length;
                const data = decode(encoded);

                const defaultClass = statusClass["__default__"] || "status-missing";
                const defaultSymbol = symbols["__default__"] || ".";

                const rowHeightEstimate = 28;
                let rowHeight = rowHeightEstimate;
                let previousStart = -1;

                renderInitial();

                container.addEventListener("scroll", () => {
                    window.requestAnimationFrame(renderVisibleRows);
                });

                function renderInitial() {
                    renderVisibleRows();
                    const sampleRow = tbody.querySelector("tr.data-row");
                    if (sampleRow) {
                        rowHeight = sampleRow.getBoundingClientRect().height || rowHeightEstimate;
                        previousStart = -1;
                        renderVisibleRows();
                    }
                }

                function renderVisibleRows() {
                    const visibleHeight = container.clientHeight;
                    const scrollTop = container.scrollTop;
                    const buffer = 20;

                    const start = Math.max(0, Math.floor(scrollTop / rowHeight) - buffer);
                    const visibleCount = Math.ceil(visibleHeight / rowHeight) + buffer * 2;
                    const end = Math.min(totalRows, start + visibleCount);

                    if (start === previousStart) {
                        return;
                    }
                    previousStart = start;

                    const topSpacer = start * rowHeight;
                    const bottomSpacer = Math.max(0, (totalRows - end) * rowHeight);
                    const colspan = Number(tbody.dataset.colspan) || (colCount + 1);

                    let html = "";

                    if (topSpacer > 0) {
                        html += `<tr class="virtual-spacer"><td colspan="${colspan}" style="height:${topSpacer}px;border:none;padding:0;"></td></tr>`;
                    }

                    for (let rowIdx = start; rowIdx < end; rowIdx++) {
                        html += buildRow(rowIdx);
                    }

                    if (bottomSpacer > 0) {
                        html += `<tr class="virtual-spacer"><td colspan="${colspan}" style="height:${bottomSpacer}px;border:none;padding:0;"></td></tr>`;
                    }

                    tbody.innerHTML = html;
                }

                function buildRow(rowIdx) {
                    const group = rows[rowIdx];
                    const rowLabel = escapeHtml(group);
                    const startOffset = rowIdx * colCount;
                    let cells = "";

                    for (let colIdx = 0; colIdx < colCount; colIdx++) {
                        const cellIndex = startOffset + colIdx;
                        const code = data[cellIndex];
                        const status = statuses[code] || "absent";
                        const cssClass = statusClass[status] || defaultClass;
                        const title = escapeHtml(status);
                        const symbol = symbols[status] !== undefined ? symbols[status] : defaultSymbol;
                        const subEntry = sub[cellIndex];

                        if (subEntry && subEntry.length) {
                            const spans = subEntry.map((subCode) => {
                                const subStatus = statuses[subCode] || "absent";
                                const subClass = statusClass[subStatus] || defaultClass;
                                return `<span class="${subClass}" title="${escapeHtml(subStatus)}"></span>`;
                            }).join("");
                            cells += `<td title="${title}"><div class="sub">${spans}</div></td>`;
                        } else {
                            cells += `<td class="${cssClass}" title="${title}">${escapeHtml(symbol)}</td>`;
                        }
                    }

                    return `<tr class="data-row"><th scope="row" class="group-col">${rowLabel}</th>${cells}</tr>`;
                }
            }

            function decode(data) {
                const binary = atob(data);
                const arr = new Uint8Array(binary.length);
                for (let i = 0; i < binary.length; i++) {
                    arr[i] = binary.charCodeAt(i);
                }
                return arr;
            }

            function escapeHtml(value) {
                return String(value)
                    .replace(/&/g, "&amp;")
                    .replace(/</g, "&lt;")
                    .replace(/>/g, "&gt;")
                    .replace(/"/g, "&quot;")
                    .replace(/'/g, "&#039;");
            }
        """

        script_calls = "\n".join(scripts)

        html_output = (
            "<html><head><meta charset='utf-8'>"
            f"<style>{style}</style>"
            "<title>Feature Availability</title></head><body>"
            "<div class='matrix-wrapper'>"
            "<div class='matrix-header'>"
            "<h1>Availability Matrix</h1>"
            f"<div class='legend'>{legend_html}</div>"
            "<div style='margin-left:auto;font-size:12px;color:#666;'>Scroll to inspect large matrices.</div>"
            "</div>"
            f"{''.join(sections)}"
            f"<script>{script}{script_calls}</script>"
            "</div>"
            "</body></html>"
        )

        with path.open("w", encoding="utf-8") as fh:
            fh.write(html_output)
