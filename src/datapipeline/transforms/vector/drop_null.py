from collections.abc import Iterator
from typing import Literal

from datapipeline.domain.sample import Sample
from datapipeline.transforms.vector_utils import is_missing

from .common import ContextExpectedMixin, select_vector


class VectorDropNullTransform(ContextExpectedMixin):
    """Drop samples when too many values are missing (None/NaN).

    Modes:
    - feature-level: enforce coverage across expected feature ids.
    - record-level: enforce cadence coverage within list/sequence features.
    """

    def __init__(
        self,
        *,
        payload: Literal["features", "targets"] = "features",
        mode: Literal["feature-level", "record-level"] = "feature-level",
        min_feature_coverage: float | None = None,
        min_cadence_coverage: float | None = None,
    ) -> None:
        super().__init__(payload=payload)
        if mode not in {"feature-level", "record-level"}:
            raise ValueError("mode must be 'feature-level' or 'record-level'")
        if min_feature_coverage is not None and not 0.0 <= min_feature_coverage <= 1.0:
            raise ValueError("min_feature_coverage must be between 0 and 1")
        if min_cadence_coverage is not None and not 0.0 <= min_cadence_coverage <= 1.0:
            raise ValueError("min_cadence_coverage must be between 0 and 1")
        self.mode = mode
        self.min_feature_coverage = min_feature_coverage
        self.min_cadence_coverage = min_cadence_coverage

    def __call__(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        return self.apply(stream)

    def apply(self, stream: Iterator[Sample]) -> Iterator[Sample]:
        for sample in stream:
            vector = select_vector(sample, self._payload)
            if vector is None:
                yield sample
                continue
            if self.mode == "feature-level":
                if self._should_drop_feature(vector):
                    continue
            else:
                if self._should_drop_cadence(vector):
                    continue
            yield sample

    def _should_drop_feature(self, vector) -> bool:
        expected = self._expected_ids()
        if not expected:
            return False
        threshold = self.min_feature_coverage
        # Allow the cadence threshold name for feature mode to keep config simple.
        if threshold is None:
            threshold = self.min_cadence_coverage
        if threshold is None:
            threshold = 1.0
        present = sum(1 for fid in expected if not is_missing(vector.values.get(fid)))
        coverage = present / len(expected)
        return coverage < threshold

    def _should_drop_cadence(self, vector) -> bool:
        threshold = self.min_cadence_coverage
        if threshold is None:
            return False
        worst = 1.0
        for value in vector.values.values():
            if not isinstance(value, list):
                continue
            if not value:
                continue
            total = len(value)
            ok = sum(1 for item in value if not is_missing(item))
            coverage = ok / total
            worst = min(worst, coverage)
        if worst == 1.0 and threshold < 1.0:
            # No list features to evaluate; treat as pass-through.
            return False
        return worst < threshold
