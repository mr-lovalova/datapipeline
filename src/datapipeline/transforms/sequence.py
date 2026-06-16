from collections import deque
from typing import Iterator

from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence


class WindowTransformer:
    def __init__(
        self,
        size: int,
        stride: int = 1,
    ) -> None:
        """Sliding windows over time-ordered feature streams.

        Parameters
        - size: window length in steps (int).
        - stride: step between windows (int number of steps).
        """

        self.size = int(size)
        self.stride = int(stride)

        if self.size <= 0 or self.stride <= 0:
            raise ValueError("size and stride must be positive")

    def __call__(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        return self.apply(stream)

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        """Build sliding windows per feature id and sequence key.

        Produces sliding windows per feature_id. Each output carries a
        list[Record] in ``records`` and the selected values in ``values``.
        """
        windows: dict[tuple, deque[FeatureRecord]] = {}
        steps: dict[tuple, int] = {}

        for fr in stream:
            key = (fr.id, fr.entity_key)
            window = windows.setdefault(key, deque(maxlen=self.size))
            step = steps.get(key, 0)
            window.append(fr)
            if len(window) == self.size and step % self.stride == 0:
                yield FeatureRecordSequence(
                    records=[item.record for item in window],
                    values=[item.value for item in window],
                    id=fr.id,
                    entity_key=fr.entity_key,
                )
            steps[key] = step + 1
