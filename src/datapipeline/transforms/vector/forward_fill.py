from collections.abc import Iterator
from datetime import datetime
from typing import Any, Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector_utils import clone, is_missing
from datapipeline.utils.time import parse_timecode

from .common import (
    VectorPostprocessBase,
    replace_vector,
    sample_time,
    select_vector,
    vector_history_key,
)


class VectorForwardFillTransform(VectorPostprocessBase):
    """Fill missing entries with the latest prior value for the same sample entity."""

    def __init__(
        self,
        *,
        max_age: str | None = None,
        payload: Literal["features", "targets", "both"] = "features",
        only: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> None:
        super().__init__(payload=payload, only=only, exclude=exclude)
        self.max_age = parse_timecode(max_age) if max_age is not None else None
        self.history: dict[tuple, tuple[Any, Any]] = {}

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
        current_time = sample_time(sample)
        data = clone(vector.values)
        updated = False
        for feature in ids:
            key = vector_history_key(sample, payload, feature)
            value = data.get(feature)
            if not is_missing(value):
                self.history[key] = (value, current_time)
                continue
            fill = self._fill_value(key, current_time)
            if fill is not None:
                data[feature] = fill
                updated = True
        if not updated:
            return sample
        return replace_vector(sample, payload, Vector(values=data))

    def _fill_value(self, key: tuple, current_time: Any) -> Any | None:
        previous = self.history.get(key)
        if previous is None:
            return None
        value, observed_time = previous
        if not self._within_max_age(current_time, observed_time):
            return None
        return value

    def _within_max_age(self, current_time: Any, observed_time: Any) -> bool:
        if self.max_age is None:
            return True
        if not isinstance(current_time, datetime) or not isinstance(
            observed_time, datetime
        ):
            raise ValueError("forward_fill max_age requires datetime sample keys")
        age = current_time - observed_time
        return age.total_seconds() >= 0 and age <= self.max_age
