from collections.abc import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.interfaces import RecordTransformBase
from datapipeline.transforms.utils import clone_record
from datapipeline.utils.time import parse_timecode


class ShiftTimeRecordTransform(RecordTransformBase):
    def __init__(self, by: str) -> None:
        self.delta = parse_timecode(by)

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        for record in stream:
            yield clone_record(record, time=record.time + self.delta)
