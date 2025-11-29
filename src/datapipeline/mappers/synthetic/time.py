from typing import Iterator
from datetime import datetime
from math import sin, pi
from datapipeline.domain.record import TemporalRecord


def encode(stream: Iterator[TemporalRecord], mode: str) -> Iterator[TemporalRecord]:
    for rec in stream:
        t: datetime = rec.time
        if mode == "hour_sin":
            val = sin(2 * pi * t.hour / 24)
        elif mode == "weekday_sin":
            val = sin(2 * pi * t.weekday() / 7)
        elif mode == "linear":
            val = t.timestamp()
        elif mode == "since_start_hours":
            first_time = None
            for rec in stream:
                t = rec.time
                if first_time is None:
                    first_time = t
                val = (t - first_time).total_seconds() / 3600.0
                yield TemporalRecord(time=rec.time, value=val)
        else:
            raise ValueError(f"Unsupported encode_time mode: {mode}")
        yield TemporalRecord(time=rec.time, value=val)
