from collections.abc import Iterator
from typing import Any, Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector_utils import clone, is_missing

from .common import VectorPostprocessBase, replace_vector, select_vector


class VectorReplaceTransform(VectorPostprocessBase):
    """Fill missing entries with a constant value."""

    def __init__(
        self,
        *,
        value: Any,
        payload: Literal["features", "targets", "both"] = "features",
        only: list[str] | None = None,
        exclude: list[str] | None = None,
        target: Any | None = None,
    ) -> None:
        super().__init__(payload=payload, only=only, exclude=exclude)
        self.value = value
        self._target = target

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            for kind in self._payload_kinds():
                ids = self._ids_for(kind)
                if ids:
                    sample = self._apply_to_payload(sample, kind, ids)
            yield sample

    def _should_replace(self, value: Any) -> bool:
        if self._target is None:
            return is_missing(value)
        return value == self._target

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
            current = data.get(feature)
            if not self._should_replace(current):
                continue
            data[feature] = self.value
            updated = True
        if not updated:
            return sample
        return replace_vector(sample, payload, Vector(values=data))
