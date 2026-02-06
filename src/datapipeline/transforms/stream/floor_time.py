from typing import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.interfaces import StreamTransformBase
from datapipeline.transforms.utils import floor_record_time


class FloorTimeTransform(StreamTransformBase):
    """Floor record timestamps to the given cadence bucket."""

    def __init__(self, cadence: str) -> None:
        self.cadence = cadence

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        for record in stream:
            yield floor_record_time(record, self.cadence)
