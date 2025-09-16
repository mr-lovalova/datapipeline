from typing import Iterator
from datapipeline.domain.feature import FeatureRecord
from itertools import groupby
from collections import deque


class TimeWindowTransformer:
    def __init__(self, size: int, stride: int = 1):
        self.size = size
        self.stride = stride

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        """Assumes input is pre-sorted by (feature_id, record.time).

        Produces sliding windows per feature_id. Each output FeatureRecord has
        a list[Record] in `record` and carries the current group_key.
        """
        grouped = groupby(stream, key=lambda fr: fr.feature_id)

        for feature_id, records in grouped:
            window = deque(maxlen=self.size)
            step = 0
            for fr in records:
                window.append(fr)
                if len(window) == self.size and step % self.stride == 0:
                    yield FeatureRecord(
                        record=[r.record for r in window],
                        feature_id=feature_id,
                        group_key=fr.group_key,
                    )
                step += 1
