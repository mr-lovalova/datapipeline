from typing import Iterator, Dict, Any, Optional
import logging
from datapipeline.sources.models.loader import SyntheticLoader
from datapipeline.sources.models.generator import DataGenerator
from datapipeline.utils.placeholders import coalesce_missing
from datapipeline.utils.time import parse_timecode, parse_datetime


class TimeTicksGenerator(DataGenerator):
    def __init__(self, start: str, end: str, frequency: str | None = "1h"):
        self.start = parse_datetime(start)
        self.end = parse_datetime(end)
        self.frequency = parse_timecode(frequency or "1h")

    def generate(self) -> Iterator[Dict[str, Any]]:
        current = self.start
        while current <= self.end:
            yield {"time": current}
            current += self.frequency

    def count(self) -> Optional[int]:
        secs = self.frequency.total_seconds()
        if secs <= 0:
            raise ValueError("frequency must be positive")
        return int((self.end - self.start).total_seconds() // secs) + 1


logger = logging.getLogger(__name__)


def make_time_loader(start: str, end: str, frequency: str | None = "1h") -> SyntheticLoader:
    """Factory entrypoint for synthetic time ticks loader.

    Returns a SyntheticLoader that wraps the TimeTicksGenerator.

    Behavior on unresolved dates:
    - Synthetic sources require explicit start/end bounds. If either `start` or
      `end` is missing or resolves to an explicit null (MissingInterpolation),
      raise a ValueError with guidance instead of silently yielding no data.
    """
    start_val = coalesce_missing(start)
    end_val = coalesce_missing(end)
    freq_val = coalesce_missing(frequency, default="1h")

    if start_val is None or end_val is None:
        raise ValueError(
            "synthetic time loader requires non-null start and end; "
            "set explicit project.globals.start_time/end_time or override source.loader.args."
        )
    return SyntheticLoader(TimeTicksGenerator(start_val, end_val, freq_val))
