from collections.abc import Iterator
from statistics import mean, median
from typing import Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector_utils import base_id, clone, is_missing

from .common import ContextExpectedMixin, replace_vector, select_vector


class VectorFillAcrossPartitionsTransform(ContextExpectedMixin):
    """Fill missing entries by aggregating sibling partitions at the same timestamp."""

    def __init__(
        self,
        *,
        statistic: Literal["mean", "median"] = "median",
        min_samples: int = 1,
        payload: Literal["features", "targets"] = "features",
    ) -> None:
        super().__init__(payload=payload)
        if min_samples <= 0:
            raise ValueError("min_samples must be positive")
        self.statistic = statistic
        self.min_samples = min_samples

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            vector = select_vector(sample, self._payload)
            if vector is None:
                yield sample
                continue
            targets = self._expected_ids()
            if not targets:
                yield sample
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
                fill = mean(candidates) if self.statistic == "mean" else median(candidates)
                data[feature] = float(fill)
                updated = True
            if updated:
                yield replace_vector(sample, self._payload, Vector(values=data))
            else:
                yield sample

