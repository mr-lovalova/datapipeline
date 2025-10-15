from __future__ import annotations

from typing import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.config.dataset.normalize import floor_time_to_resolution


def floor_time(stream: Iterator[TemporalRecord], resolution: str) -> Iterator[TemporalRecord]:
    """Floor record timestamps to the given resolution (e.g., '1h', '10min').

    Useful before granularity aggregation to downsample within bins by making
    all intra-bin records share the same timestamp.
    """
    for record in stream:
        record.time = floor_time_to_resolution(record.time, resolution)
        yield record
