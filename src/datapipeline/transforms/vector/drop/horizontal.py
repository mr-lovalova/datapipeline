from __future__ import annotations

from collections.abc import Iterator
from typing import Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector_utils import is_missing

from ..common import VectorPostprocessBase, select_vector


def cell_coverage(value) -> float:
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


class VectorDropHorizontalTransform(VectorPostprocessBase):
    """Horizontal (row-wise) drop based on coverage thresholds."""

    def __init__(
        self,
        *,
        threshold: float,
        payload: Literal["features", "targets", "both"] = "features",
        only: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> None:
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("threshold must be between 0 and 1.")
        super().__init__(payload=payload, only=only, exclude=exclude)
        self._threshold = threshold

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            total = 0.0
            count = 0
            for kind in self._payload_kinds():
                baseline = self._ids_for(kind)
                if not baseline:
                    continue
                vector = select_vector(sample, kind)
                if vector is None:
                    continue
                total += self._horizontal_coverage(vector, baseline) * len(baseline)
                count += len(baseline)
            if count == 0:
                yield sample
                continue
            coverage = total / float(count)
            if coverage < self._threshold:
                continue
            yield sample

    @staticmethod
    def _horizontal_coverage(vector: Vector, baseline: list[str]) -> float:
        if not baseline:
            return 1.0
        total = 0.0
        for fid in baseline:
            value = vector.values.get(fid)
            total += cell_coverage(value)
        return total / float(len(baseline))

