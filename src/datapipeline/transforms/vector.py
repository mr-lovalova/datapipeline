from __future__ import annotations
from collections import deque
from collections.abc import Iterator, Mapping, MutableMapping, Sequence
from statistics import mean, median
from typing import Any, Literal, Tuple

from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector_utils import (
    base_id as _base_id,
    is_missing as _is_missing,
    normalize_key as _normalize_key,
    resolve_expected as _resolve_expected,
)


def _clone(values: Mapping[str, Any]) -> MutableMapping[str, Any]:
    if isinstance(values, MutableMapping):
        return type(values)(values)
    return dict(values)


def _base(feature_id: str) -> str:
    return _base_id(feature_id)


class _ExpectedFeaturesMixin:
    def __init__(
        self,
        *,
        expected: Sequence[str] | None = None,
        manifest: str | None = None,
        match_partition: Literal["base", "full"] = "base",
    ) -> None:
        self._expected = _resolve_expected(
            manifest, expected, match_partition=match_partition)

    @property
    def expected(self) -> list[str]:
        return list(self._expected)


class VectorDropMissingTransform(_ExpectedFeaturesMixin):
    """Drop vectors that do not satisfy coverage requirements."""

    def __init__(
        self,
        *,
        required: Sequence[str] | None = None,
        expected: Sequence[str] | None = None,
        manifest: str | None = None,
        min_coverage: float = 1.0,
        match_partition: Literal["base", "full"] = "full",
    ) -> None:
        super().__init__(expected=expected, manifest=manifest, match_partition=match_partition)
        if not 0.0 <= min_coverage <= 1.0:
            raise ValueError("min_coverage must be between 0 and 1")
        self.required = {str(item) for item in (required or [])}
        self.min_coverage = min_coverage
        self.match_partition = match_partition

    def _normalize(self, feature_id: str) -> str:
        return _normalize_key(feature_id, self.match_partition)

    def apply(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        baseline = set(self.required or self.expected)
        for group_key, vector in stream:
            present = {
                self._normalize(fid)
                for fid, value in vector.values.items()
                if not _is_missing(value)
            }
            if self.required and not self.required.issubset(present):
                continue
            if baseline:
                coverage = len(present & baseline) / len(baseline)
                if coverage < self.min_coverage:
                    continue
            yield group_key, vector


class VectorFillConstantTransform(_ExpectedFeaturesMixin):
    """Fill missing entries with a constant value."""

    def __init__(
        self,
        *,
        value: Any,
        expected: Sequence[str] | None = None,
        manifest: str | None = None,
    ) -> None:
        super().__init__(expected=expected, manifest=manifest, match_partition="base")
        self.value = value

    def apply(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        targets = self.expected
        for group_key, vector in stream:
            if not targets:
                yield group_key, vector
                continue
            data = _clone(vector.values)
            updated = False
            for feature in targets:
                if feature not in data or _is_missing(data[feature]):
                    data[feature] = self.value
                    updated = True
            if updated:
                yield group_key, Vector(values=data)
            else:
                yield group_key, vector


class VectorFillHistoryTransform(_ExpectedFeaturesMixin):
    """Fill missing entries using running statistics from prior buckets.

    When `match_partition` is "full" and a manifest is provided (with no
    explicit `expected` list), targets are taken from `manifest.partitions` to
    operate per-partition. Otherwise, defaults to base feature ids from
    `manifest.features`.
    """

    def __init__(
        self,
        *,
        statistic: Literal["mean", "median"] = "median",
        window: int | None = None,
        min_samples: int = 1,
        expected: Sequence[str] | None = None,
        manifest: str | None = None,
        match_partition: Literal["base", "full"] = "full",
    ) -> None:
        super().__init__(expected=expected, manifest=manifest, match_partition=match_partition)
        if window is not None and window <= 0:
            raise ValueError("window must be positive when provided")
        if min_samples <= 0:
            raise ValueError("min_samples must be positive")
        self.statistic = statistic
        self.window = window
        self.min_samples = min_samples
        self.history: dict[str, deque[float]] = {}
        self.match_partition = match_partition

    def _compute(self, feature_id: str) -> float | None:
        key = _normalize_key(feature_id, self.match_partition)
        values = self.history.get(key)
        if not values or len(values) < self.min_samples:
            return None
        if self.statistic == "mean":
            return float(mean(values))
        return float(median(values))

    def _push(self, feature_id: str, value: Any) -> None:
        if _is_missing(value):
            return
        try:
            num = float(value)
        except (TypeError, ValueError):
            # Ignore non-scalar/non-numeric entries
            return
        key = _normalize_key(feature_id, self.match_partition)
        bucket = self.history.setdefault(key, deque(maxlen=self.window))
        bucket.append(num)

    def apply(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        targets = self.expected
        for group_key, vector in stream:
            data = _clone(vector.values)
            updated = False
            for feature in targets:
                if feature in data and not _is_missing(data[feature]):
                    continue
                fill = self._compute(feature)
                if fill is not None:
                    data[feature] = fill
                    updated = True
            # Push history after possibly filling
            for fid, value in data.items():
                self._push(fid, value)
            if updated:
                yield group_key, Vector(values=data)
            else:
                yield group_key, vector


class VectorFillAcrossPartitionsTransform(_ExpectedFeaturesMixin):
    """Fill missing entries by aggregating sibling partitions at the same timestamp.

    When operating with `match_partition="full"` and a manifest is provided (and
    no explicit `expected` list), targets are taken from `manifest.partitions`
    so that filling addresses concrete partition IDs.
    """

    def __init__(
        self,
        *,
        statistic: Literal["mean", "median"] = "median",
        min_samples: int = 1,
        expected: Sequence[str] | None = None,
        manifest: str | None = None,
        match_partition: Literal["base", "full"] = "full",
    ) -> None:
        super().__init__(expected=expected, manifest=manifest, match_partition=match_partition)
        if min_samples <= 0:
            raise ValueError("min_samples must be positive")
        self.statistic = statistic
        self.min_samples = min_samples
        self.match_partition = match_partition

    def apply(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        targets = self.expected
        for group_key, vector in stream:
            if not targets:
                yield group_key, vector
                continue

            data = _clone(vector.values)
            base_groups: dict[str, list[float]] = {}
            for fid, value in data.items():
                if _is_missing(value):
                    continue
                try:
                    num = float(value)
                except (TypeError, ValueError):
                    continue
                base_groups.setdefault(_base(fid), []).append(num)

            updated = False
            for feature in targets:
                if feature in data and not _is_missing(data[feature]):
                    continue
                base = _base(feature)
                candidates = base_groups.get(base, [])
                if len(candidates) < self.min_samples:
                    continue
                fill = mean(candidates) if self.statistic == "mean" else median(
                    candidates)
                data[feature] = float(fill)
                updated = True
            if updated:
                yield group_key, Vector(values=data)
            else:
                yield group_key, vector
