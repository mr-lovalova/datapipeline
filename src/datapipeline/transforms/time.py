from collections.abc import Iterator
from datetime import datetime, timezone

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.utils import clone_record
from datapipeline.utils.time import parse_cadence, parse_timecode


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class FloorTimeTransform:
    """Floor record timestamps to a fixed UTC cadence."""

    def __init__(self, cadence: str) -> None:
        self.step = parse_cadence(cadence)

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        for record in stream:
            time = _EPOCH + ((record.time - _EPOCH) // self.step) * self.step
            yield clone_record(record, time=time)


class ShiftTimeTransform:
    """Shift record timestamps by a fixed duration."""

    def __init__(self, by: str) -> None:
        self.delta = parse_timecode(by)

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        for record in stream:
            yield clone_record(record, time=record.time + self.delta)
