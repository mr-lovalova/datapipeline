from typing import Iterator, Dict, Any, Optional
from datapipeline.sources.models.loader import SyntheticLoader
from datapipeline.sources.models.generator import DataGenerator
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


def make_time_loader(start: str, end: str, frequency: str | None = "1h") -> SyntheticLoader:
    """Factory entrypoint for synthetic time ticks loader.

    Returns a SyntheticLoader that wraps the TimeTicksGenerator.
    """
    return SyntheticLoader(TimeTicksGenerator(start, end, frequency))
