from collections import deque
from collections.abc import Iterator
from statistics import mean, median
from typing import Any, Literal, Tuple

from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector_utils import base_id, is_missing, clone
from datapipeline.pipeline.context import PipelineContext, try_get_current_context


class _ContextExpectedMixin:
    def __init__(self) -> None:
        self._context: PipelineContext | None = None

    def bind_context(self, context: PipelineContext) -> None:
        self._context = context

    def _expected_ids(self) -> list[str]:
        ctx = self._context or try_get_current_context()
        if not ctx:
            return []
        return ctx.load_expected_ids()


class VectorDropMissingTransform(_ContextExpectedMixin):
    """Drop vectors that do not satisfy coverage requirements."""

    def __init__(
        self,
        *,
        required: list[str] | None = None,
        min_coverage: float = 1.0,
    ) -> None:
        super().__init__()
        if not 0.0 <= min_coverage <= 1.0:
            raise ValueError("min_coverage must be between 0 and 1")
        self.required = {str(item) for item in (required or [])}
        self.min_coverage = min_coverage
        # Always operate on full (partition) ids

    def __call__(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        for group_key, vector in stream:
            present = {fid for fid, value in vector.values.items()
                       if not is_missing(value)}
            # Enforce hard requirements first (normalize required keys for fair comparison)
            if self.required:
                if not set(self.required).issubset(present):
                    continue

            # Coverage baseline uses explicit expected if provided; otherwise dynamic set
            baseline = set(self._expected_ids())
            if baseline:
                coverage = len(present & baseline) / len(baseline)
                if coverage < self.min_coverage:
                    continue
            yield group_key, vector


class VectorFillConstantTransform(_ContextExpectedMixin):
    """Fill missing entries with a constant value."""

    def __init__(
        self,
        *,
        value: Any,
    ) -> None:
        super().__init__()
        self.value = value

    def __call__(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        for group_key, vector in stream:
            targets = self._expected_ids()
            if not targets:
                yield group_key, vector
                continue
            data = clone(vector.values)
            updated = False
            for feature in targets:
                if feature not in data or is_missing(data[feature]):
                    data[feature] = self.value
                    updated = True
            if updated:
                yield group_key, Vector(values=data)
            else:
                yield group_key, vector


class VectorFillHistoryTransform(_ContextExpectedMixin):
    """Fill missing entries using running statistics from prior buckets."""

    def __init__(
        self,
        *,
        statistic: Literal["mean", "median"] = "median",
        window: int | None = None,
        min_samples: int = 1,
    ) -> None:
        super().__init__()
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
        if is_missing(value):
            return
        try:
            num = float(value)
        except (TypeError, ValueError):
            # Ignore non-scalar/non-numeric entries
            return
        bucket = self.history.setdefault(
            str(feature_id), deque(maxlen=self.window))
        bucket.append(num)

    def __call__(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        for group_key, vector in stream:
            targets = self._expected_ids()
            data = clone(vector.values)
            updated = False
            for feature in targets:
                if feature in data and not is_missing(data[feature]):
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


class VectorFillAcrossPartitionsTransform(_ContextExpectedMixin):
    """Fill missing entries by aggregating sibling partitions at the same timestamp."""

    def __init__(
        self,
        *,
        statistic: Literal["mean", "median"] = "median",
        min_samples: int = 1,
    ) -> None:
        super().__init__()
        if min_samples <= 0:
            raise ValueError("min_samples must be positive")
        self.statistic = statistic
        self.min_samples = min_samples
        # Always operate on full (partition) ids

    def __call__(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Tuple[Any, Vector]]) -> Iterator[Tuple[Any, Vector]]:
        for group_key, vector in stream:
            targets = self._expected_ids()
            if not targets:
                yield group_key, vector
                continue

            data = clone(vector.values)
            base_groups: dict[str, list[float]] = {}
            for fid, value in data.items():
                if is_missing(value):
                    continue
                try:
                    num = float(value)
                except (TypeError, ValueError):
                    continue
                base_groups.setdefault(base_id(fid), []).append(num)

            updated = False
            for feature in targets:
                if feature in data and not is_missing(data[feature]):
                    continue
                base = base_id(feature)
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
