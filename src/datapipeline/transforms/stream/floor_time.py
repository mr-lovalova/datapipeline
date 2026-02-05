from typing import Iterator

from datapipeline.domain.feature import FeatureRecord
from datapipeline.transforms.interfaces import StreamTransformBase
from datapipeline.transforms.utils import floor_record_time
from datapipeline.transforms.stream.adapters import map_record_stream


class FloorTimeTransform(StreamTransformBase):
    """Floor feature record timestamps to the given cadence bucket."""

    def __init__(self, cadence: str) -> None:
        self.cadence = cadence

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        def apply(record):
            return floor_record_time(record, self.cadence)

        return map_record_stream(stream, apply)
