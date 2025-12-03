from __future__ import annotations

from collections.abc import Iterator
from typing import Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector_utils import is_missing

from .common import ContextExpectedMixin, select_vector, replace_vector, try_get_current_context
from .drop_partitions import VectorDropPartitionsTransform


Axis = Literal["horizontal", "vertical"]


def _cell_coverage(value) -> float:
    """Return coverage for a single feature value.

    Scalars: 1.0 when not missing, 0.0 when missing.
    Lists: fraction of non-missing elements (0.0 for empty lists).
    """
    if isinstance(value, list):
        if not value:
            return 0.0
        total = len(value)
        ok = sum(1 for item in value if not is_missing(item))
        return ok / total if total > 0 else 0.0
    if is_missing(value):
        return 0.0
    return 1.0


class VectorDropTransform(ContextExpectedMixin):
    """Drop vectors or features based on coverage thresholds.

    axis="horizontal" drops individual samples/vectors whose averaged per-feature
    coverage falls below the configured threshold.

    axis="vertical" drops features/partitions whose global coverage across the
    partition falls below the threshold, delegating to VectorDropPartitionsTransform.
    """

    def __init__(
        self,
        *,
        axis: Axis = "horizontal",
        threshold: float,
        payload: Literal["features", "targets"] = "features",
    ) -> None:
        super().__init__(payload=payload)
        if axis not in {"horizontal", "vertical"}:
            raise ValueError("axis must be 'horizontal' or 'vertical'")
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("threshold must be between 0 and 1.")
        self._axis: Axis = axis
        self._threshold: float = threshold
        self._baseline: list[str] | None = None
        self._vertical_delegate: VectorDropPartitionsTransform | None = None

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def bind_context(self, context) -> None:
        super().bind_context(context)
        if self._axis == "vertical":
            delegate = VectorDropPartitionsTransform(
                payload=self._payload, threshold=self._threshold
            )
            delegate.bind_context(context)
            self._vertical_delegate = delegate

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        if self._axis == "vertical":
            delegate = self._resolve_vertical_delegate()
            yield from delegate.apply(stream)
            return

        baseline = self._resolve_baseline()
        if not baseline:
            # No expected ids to evaluate against; pass-through.
            yield from stream
            return

        for sample in stream:
            vector = select_vector(sample, self._payload)
            if vector is None:
                yield sample
                continue
            coverage = self._horizontal_coverage(vector, baseline)
            if coverage < self._threshold:
                continue
            yield sample

    def _resolve_baseline(self) -> list[str]:
        if self._baseline is not None:
            return list(self._baseline)
        ids = self._expected_ids()
        self._baseline = list(ids)
        return list(self._baseline)

    def _horizontal_coverage(self, vector: Vector, baseline: list[str]) -> float:
        if not baseline:
            return 1.0
        total = 0.0
        for fid in baseline:
            value = vector.values.get(fid)
            total += _cell_coverage(value)
        return total / float(len(baseline))

    def _resolve_vertical_delegate(self) -> VectorDropPartitionsTransform:
        if self._vertical_delegate is not None:
            return self._vertical_delegate
        ctx = self._context or try_get_current_context()
        delegate = VectorDropPartitionsTransform(
            payload=self._payload, threshold=self._threshold
        )
        if ctx is not None:
            delegate.bind_context(ctx)
        self._vertical_delegate = delegate
        return delegate

