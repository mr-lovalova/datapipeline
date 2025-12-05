from collections import deque
from collections.abc import Iterator
from statistics import mean, median
from typing import Any, Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector_utils import clone, is_missing

from .common import VectorPostprocessBase, replace_vector, select_vector


class VectorFillTransform(VectorPostprocessBase):
    """Fill missing entries using running statistics from prior buckets."""

    def __init__(
        self,
        *,
        statistic: Literal["mean", "median"] = "median",
        window: int | None = None,
        min_samples: int = 1,
        payload: Literal["features", "targets", "both"] = "features",
        only: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> None:
        super().__init__(payload=payload, only=only, exclude=exclude)
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
            return
        bucket = self.history.setdefault(str(feature_id), deque(maxlen=self.window))
        bucket.append(num)

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            for kind in self._payload_kinds():
                ids = self._ids_for(kind)
                if ids:
                    sample = self._apply_to_payload(sample, kind, ids)
            yield sample

    def _apply_to_payload(
        self,
        sample: Sample,
        payload: Literal["features", "targets"],
        ids: list[str],
    ) -> Sample:
        vector = select_vector(sample, payload)
        if vector is None:
            return sample
        data = clone(vector.values)
        updated = False
        for feature in ids:
            if feature in data and not is_missing(data[feature]):
                continue
            fill = self._compute(feature)
            if fill is not None:
                data[feature] = fill
                updated = True
        for fid, value in data.items():
            self._push(fid, value)
        if not updated:
            return sample
        return replace_vector(sample, payload, Vector(values=data))
