from __future__ import annotations
from collections import Counter, defaultdict
from typing import Any, Hashable, Iterable, Literal
from datapipeline.transforms.vector_utils import base_id as _base_id
from datetime import datetime
from pathlib import Path


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
            rendered += ", ..."
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
        from .matrix import render_matrix

        render_matrix(
            self,
            features=features,
            partitions=partitions,
            column_width=column_width,
        )

    def print_report(self) -> dict[str, Any]:
        from .report import print_report as _print_report

        return _print_report(self)

    def _export_matrix_data(self) -> None:
        from .matrix import export_matrix_data

        export_matrix_data(self)

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
