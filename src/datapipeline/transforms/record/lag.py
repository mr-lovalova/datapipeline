from __future__ import annotations

from datetime import timedelta
from typing import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.utils.time import parse_timecode


def _shift_record_time(record: TemporalRecord, lag: timedelta) -> TemporalRecord:
    record.time = record.time - lag
    return record


def apply_lag(stream: Iterator[TemporalRecord], lag: str) -> Iterator[TemporalRecord]:
    lag_td = parse_timecode(lag)
    for record in stream:
        yield _shift_record_time(record, lag_td)
