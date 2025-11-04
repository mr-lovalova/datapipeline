from collections.abc import Iterator
from typing import Any, Literal

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector_utils import clone, is_missing

from .common import ContextExpectedMixin, replace_vector, select_vector


class VectorFillConstantTransform(ContextExpectedMixin):
    """Fill missing entries with a constant value."""

    def __init__(
        self,
        *,
        value: Any,
        payload: Literal["features", "targets"] = "features",
    ) -> None:
        super().__init__(payload=payload)
        self.value = value

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
            updated = False
            for feature in targets:
                if feature not in data or is_missing(data[feature]):
                    data[feature] = self.value
                    updated = True
            if updated:
                yield replace_vector(sample, self._payload, Vector(values=data))
            else:
                yield sample

