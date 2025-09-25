from __future__ import annotations

from datetime import timedelta
from typing import Any, Iterator, Mapping

from datapipeline.domain.record import TimeSeriesRecord
from datapipeline.utils.time import parse_timecode


def _get_field(record: Any, field: str, default: Any = None) -> Any:
    if isinstance(record, Mapping):
        return record.get(field, default)
    return getattr(record, field, default)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        return value != value
    return False


def shift_record_time(record: TimeSeriesRecord, lag: timedelta) -> TimeSeriesRecord:
    record.time = record.time - lag
    return record


def time_lag(stream: Iterator[TimeSeriesRecord], lag: str) -> Iterator[TimeSeriesRecord]:
    lag_td = parse_timecode(lag)
    for record in stream:
        yield shift_record_time(record, lag_td)


def drop_missing_values(
    stream: Iterator[Any],
    field: str = "value",
) -> Iterator[Any]:
    for record in stream:
        value = _get_field(record, field)
        if _is_missing(value):
            continue
        yield record
