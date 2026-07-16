from collections import deque
from collections.abc import Iterator
from itertools import groupby

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.utils import (
    clone_record_with_field,
    get_field,
    partition_key,
)


class LagTransform:
    def __init__(
        self,
        field: str,
        periods: int,
        partition_fields: tuple[str, ...],
        to: str | None = None,
    ) -> None:
        self.field = field
        self.to = field if to is None else to
        self.periods = periods
        self.partition_fields = partition_fields

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        for _, records in groupby(
            stream,
            key=lambda record: partition_key(record, self.partition_fields),
        ):
            previous: deque[object] = deque(maxlen=self.periods)
            for record in records:
                value = previous[0] if len(previous) == self.periods else None
                previous.append(get_field(record, self.field))
                yield clone_record_with_field(record, self.to, value)
