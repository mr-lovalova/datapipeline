from collections import deque
from collections.abc import Iterator
from itertools import groupby
from typing import Literal

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.interfaces import FieldValueStreamTransformBase
from datapipeline.transforms.utils import clone_record_with_field, get_field


class PeriodShiftTransformer(FieldValueStreamTransformBase):
    def __init__(
        self,
        *,
        field: str,
        periods: int,
        direction: Literal["lag", "lead"],
        to: str | None = None,
        partition_by: str | list[str] | None = None,
    ) -> None:
        super().__init__(field=field, to=to, partition_by=partition_by)
        if periods <= 0:
            raise ValueError("periods must be a positive integer")
        self.periods = periods
        self.direction = direction

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        grouped = groupby(stream, key=self.partition_key)
        for _, records in grouped:
            if self.direction == "lag":
                yield from self._lag(records)
            else:
                yield from self._lead(records)

    def _lag(self, records: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        previous: deque[object] = deque(maxlen=self.periods)
        for record in records:
            value = previous[0] if len(previous) == self.periods else None
            previous.append(get_field(record, self.field))
            yield clone_record_with_field(record, self.to, value)

    def _lead(self, records: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        future: deque[TemporalRecord] = deque()
        source = iter(records)
        for _ in range(self.periods):
            try:
                future.append(next(source))
            except StopIteration:
                break

        for record in source:
            lead_record = future.popleft()
            future.append(record)
            yield clone_record_with_field(
                lead_record,
                self.to,
                get_field(record, self.field),
            )

        while future:
            yield clone_record_with_field(future.popleft(), self.to, None)
