"""Vector-level pipeline transforms for time-series datasets."""

from __future__ import annotations

import json
from collections import deque
from collections.abc import Iterator, Mapping, MutableMapping, Sequence
from pathlib import Path
from statistics import mean, median
from typing import Any, Literal, Tuple

from datapipeline.domain.vector import Vector

_MANIFEST_CACHE: dict[Path, dict[str, Any]] = {}


def _load_manifest(path: Path) -> dict[str, Any]:
    normalized = path.expanduser().resolve()
    cached = _MANIFEST_CACHE.get(normalized)
    if cached is not None:
        return cached

    with normalized.open("r", encoding="utf-8") as fh:
        data = json.load(fh)

    if not isinstance(data, Mapping):
        raise TypeError("Partition manifest must be a mapping")

    _MANIFEST_CACHE[normalized] = data
    return data


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        return value != value  # NaN
    return False


def _clone(values: Mapping[str, Any]) -> MutableMapping[str, Any]:
    if isinstance(values, MutableMapping):
        return type(values)(values)
    return dict(values)


def _base(feature_id: str) -> str:
    return feature_id.split("__", 1)[0] if "__" in feature_id else feature_id


class _ExpectedFeaturesMixin:
    def __init__(self, *, expected: Sequence[str] | None = None, manifest: str | None = None) -> None:
        targets: list[str]
        if expected:
            targets = [str(item) for item in expected]
        elif manifest:
            manifest_path = Path(manifest)
            payload = _load_manifest(manifest_path)
            raw = payload.get("features")
            if not isinstance(raw, Sequence):
                raise TypeError(
                    "Partition manifest must contain a 'features' list")
            targets = [str(item) for item in raw]
        else:
            targets = []
        self._expected = targets

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
        super().__init__(expected=expected, manifest=manifest)
        if not 0.0 <= min_coverage <= 1.0:
            raise ValueError("min_coverage must be between 0 and 1")
        self.required = {str(item) for item in (required or [])}
        self.min_coverage = min_coverage
        self.match_partition = match_partition

    def _normalize(self, feature_id: str) -> str:
        return feature_id if self.match_partition == "full" else _base(feature_id)

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
        super().__init__(expected=expected, manifest=manifest)
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
    """Fill missing entries using running statistics from prior buckets."""

    def __init__(
        self,
        *,
        statistic: Literal["mean", "median"] = "median",
        window: int | None = None,
        min_samples: int = 1,
        expected: Sequence[str] | None = None,
        manifest: str | None = None,
    ) -> None:
        super().__init__(expected=expected, manifest=manifest)
        if window is not None and window <= 0:
            raise ValueError("window must be positive when provided")
        if min_samples <= 0:
            raise ValueError("min_samples must be positive")
        self.statistic = statistic
        self.window = window
        self.min_samples = min_samples
        self.history: dict[str, deque[float]] = {}

    def _compute(self, feature_id: str) -> float | None:
        values = self.history.get(feature_id)
        if not values or len(values) < self.min_samples:
            return None
        if self.statistic == "mean":
            return float(mean(values))
        return float(median(values))

    def _push(self, feature_id: str, value: Any) -> None:
        if _is_missing(value):
            return
        num = float(value)
        bucket = self.history.setdefault(feature_id, deque(maxlen=self.window))
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
    """Fill missing entries by aggregating sibling partitions at the same timestamp."""

    def __init__(
        self,
        *,
        statistic: Literal["mean", "median"] = "median",
        min_samples: int = 1,
        expected: Sequence[str] | None = None,
        manifest: str | None = None,
    ) -> None:
        super().__init__(expected=expected, manifest=manifest)
        if min_samples <= 0:
            raise ValueError("min_samples must be positive")
        self.statistic = statistic
        self.min_samples = min_samples

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
                base_groups.setdefault(_base(fid), []).append(float(value))

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
