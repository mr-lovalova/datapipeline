from datapipeline.domain.record import TimeFeatureRecord
from datetime import timedelta
from typing import Iterator
from datapipeline.utils.time import parse_timecode


def shift_record_time(record: TimeFeatureRecord, lag: timedelta) -> TimeFeatureRecord:
    record.time = record.time - lag
    return record


def time_lag(stream: Iterator[TimeFeatureRecord], lag: str) -> Iterator[TimeFeatureRecord]:
    lag_td = parse_timecode(lag)
    for record in stream:
        yield shift_record_time(record, lag_td)
