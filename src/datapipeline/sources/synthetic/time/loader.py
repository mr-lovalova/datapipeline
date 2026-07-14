from collections.abc import Iterator
from typing import Any

from datapipeline.sources.models.generator import DataGenerator
from datapipeline.sources.models.loader import SyntheticLoader
from datapipeline.utils.placeholders import coalesce_missing
from datapipeline.utils.time import parse_datetime, parse_timecode


class TimeTicksGenerator(DataGenerator):
    def __init__(self, start: str, end: str, frequency: str | None = "1h"):
        self.start = parse_datetime(start)
        self.end = parse_datetime(end)
        self.frequency = parse_timecode(frequency or "1h")
        if self.frequency.total_seconds() <= 0:
            raise ValueError("frequency must be positive")
        if self.end < self.start:
            raise ValueError("end must not precede start")

    def generate(self) -> Iterator[dict[str, Any]]:
        current = self.start
        while current <= self.end:
            yield {"time": current}
            current += self.frequency

    def count(self) -> int:
        seconds = self.frequency.total_seconds()
        return int((self.end - self.start).total_seconds() // seconds) + 1


def make_time_loader(
    start: str,
    end: str,
    frequency: str | None = "1h",
) -> SyntheticLoader:
    """Build a bounded synthetic time source."""
    start_value = coalesce_missing(start)
    end_value = coalesce_missing(end)
    frequency_value = coalesce_missing(frequency, default="1h")

    if start_value is None or end_value is None:
        raise ValueError(
            "synthetic time loader requires non-null start and end; "
            "set explicit project.globals.start_time/end_time or override source.loader.args."
        )
    return SyntheticLoader(TimeTicksGenerator(start_value, end_value, frequency_value))
