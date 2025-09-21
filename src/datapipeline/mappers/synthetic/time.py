from typing import Iterator
from datetime import datetime
from math import sin, pi
from datapipeline.domain.record import TimeFeatureRecord


def encode(stream: Iterator[TimeFeatureRecord], mode: str) -> Iterator[TimeFeatureRecord]:
    for rec in stream:
        t: datetime = rec.time
        if mode == "hour_sin":
            val = sin(2 * pi * t.hour / 24)
        elif mode == "weekday_sin":
            val = sin(2 * pi * t.weekday() / 7)
        elif mode == "linear":
            val = t.timestamp()
        else:
            raise ValueError(f"Unsupported encode_time mode: {mode}")
        yield TimeFeatureRecord(time=rec.time, value=val)
