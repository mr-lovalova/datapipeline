from __future__ import annotations

from collections import deque
from itertools import groupby
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
        """Assumes input is pre-sorted by (feature_id, record.time).

        Produces sliding windows per feature_id. Each output carries a
        list[Record] in ``records``.
        """

        grouped = groupby(stream, key=lambda fr: fr.id)

        for fid, records in grouped:
            window = deque(maxlen=self.size)
            step = 0
            for fr in records:
                window.append(fr)
                if len(window) == self.size and step % self.stride == 0:
                    yield FeatureRecordSequence(
                        records=[r.record for r in window],
                        id=fid,
                    )
                step += 1
