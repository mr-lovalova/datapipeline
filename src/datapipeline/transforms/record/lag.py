from datetime import timedelta
from typing import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.utils.time import parse_timecode
from datapipeline.transforms.interfaces import RecordTransformBase


class LagRecordTransform(RecordTransformBase):
    def __init__(self, lag: str) -> None:
        self.lag = parse_timecode(lag)

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        for record in stream:
            record.time = record.time - self.lag
            yield record
