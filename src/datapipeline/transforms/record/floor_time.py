from __future__ import annotations

from typing import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.interfaces import RecordTransformBase
from datapipeline.transforms.utils import floor_record_time


class FloorTimeRecordTransform(RecordTransformBase):
    """Floor record timestamps to the given cadence bucket (e.g., '1h', '10min').

    Useful before granularity aggregation to downsample within bins by making
    all intra-bin records share the same timestamp.
    """

    def __init__(self, cadence: str) -> None:
        self.cadence = cadence

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        for record in stream:
            yield floor_record_time(record, self.cadence)
