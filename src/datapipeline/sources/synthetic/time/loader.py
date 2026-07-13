from collections.abc import Iterator
from datetime import timezone
from typing import Any

from datapipeline.sources.models.loader import SyntheticLoader
from datapipeline.sources.models.generator import DataGenerator
from datapipeline.utils.placeholders import coalesce_missing
from datapipeline.utils.time import parse_datetime, parse_timecode


class TimeTicksGenerator(DataGenerator):
    def __init__(self, start: str, end: str, frequency: str | None = "1h"):
        self.start = parse_datetime(start)
        self.end = parse_datetime(end)
        self.frequency = parse_timecode(frequency or "1h")
        if self.frequency.total_seconds() <= 0:
            raise ValueError("frequency must be positive")

    def generate(self) -> Iterator[dict[str, Any]]:
        current = self.start
        while current <= self.end:
            yield {"time": current}
            current += self.frequency

    def count(self) -> int:
        seconds = self.frequency.total_seconds()
        return int((self.end - self.start).total_seconds() // seconds) + 1

    def info_lines(self) -> list[str]:
        def _format_utc(value):
            return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        seconds = int(self.frequency.total_seconds())
        if seconds % 86400 == 0:
            frequency_text = f"{seconds // 86400}d"
        elif seconds % 3600 == 0:
            frequency_text = f"{seconds // 3600}h"
        elif seconds % 60 == 0:
            frequency_text = f"{seconds // 60}m"
        else:
            frequency_text = f"{seconds}s"

        return [
            "synthetic.generate: "
            f"start={_format_utc(self.start)} "
            f"end={_format_utc(self.end)} "
            f"freq={frequency_text}"
        ]


def make_time_loader(
    start: str,
    end: str,
    frequency: str | None = "1h",
) -> SyntheticLoader:
    """Factory entrypoint for synthetic time ticks loader.

    Returns a SyntheticLoader that wraps the TimeTicksGenerator.

    Behavior on unresolved dates:
    - Synthetic sources require explicit start/end bounds. If either `start` or
      `end` is missing or resolves to an explicit null (MissingInterpolation),
      raise a ValueError with guidance instead of silently yielding no data.
    """
    start_value = coalesce_missing(start)
    end_value = coalesce_missing(end)
    frequency_value = coalesce_missing(frequency, default="1h")

    if start_value is None or end_value is None:
        raise ValueError(
            "synthetic time loader requires non-null start and end; "
            "set explicit project.globals.start_time/end_time or override source.loader.args."
        )
    return SyntheticLoader(
        TimeTicksGenerator(start_value, end_value, frequency_value)
    )
