from typing import Iterator

from datapipeline.domain.feature import FeatureRecord
from datapipeline.utils.time import parse_timecode
from datapipeline.transforms.interfaces import StreamTransformBase
from datapipeline.transforms.stream.adapters import map_record_stream


class LagTransform(StreamTransformBase):
    """Shift feature record timestamps backwards by the given lag."""

    def __init__(self, lag: str) -> None:
        self.lag = parse_timecode(lag)

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        def apply(record):
            record.time = record.time - self.lag
            return record

        return map_record_stream(stream, apply)

