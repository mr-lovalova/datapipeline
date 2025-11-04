from collections.abc import Iterator
from typing import Literal

from datapipeline.domain.sample import Sample
from datapipeline.transforms.vector_utils import is_missing

from .common import ContextExpectedMixin, select_vector


class VectorDropMissingTransform(ContextExpectedMixin):
    """Drop vectors that do not satisfy coverage requirements."""

    def __init__(
        self,
        *,
        required: list[str] | None = None,
        min_coverage: float = 1.0,
        payload: Literal["features", "targets"] = "features",
    ) -> None:
        super().__init__(payload=payload)
        if not 0.0 <= min_coverage <= 1.0:
            raise ValueError("min_coverage must be between 0 and 1")
        self.required = {str(item) for item in (required or [])}
        self.min_coverage = min_coverage

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            vector = select_vector(sample, self._payload)
            if vector is None:
                yield sample
                continue
            present = {fid for fid, value in vector.values.items() if not is_missing(value)}
            if self.required and not set(self.required).issubset(present):
                continue

            baseline = set(self._expected_ids())
            if baseline:
                coverage = len(present & baseline) / len(baseline)
                if coverage < self.min_coverage:
                    continue
            yield sample

