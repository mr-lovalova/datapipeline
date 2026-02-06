from typing import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.interfaces import FieldStreamTransformBase
from datapipeline.transforms.utils import clone_record, get_field, partition_key
from datapipeline.utils.time import parse_timecode


class EnsureCadenceTransform(FieldStreamTransformBase):
    """Insert placeholder records so timestamps are exactly one cadence apart per partition.

    - cadence: duration string (e.g., "10m", "1h", "30s").
    - Placeholders carry field=None and inherit partition metadata.
    - Assumes input sorted by (partition_key, record.time).
    """

    def __init__(
        self,
        *,
        cadence: str,
        field: str,
        to: str | None = None,
        partition_by: str | list[str] | None = None,
    ) -> None:
        super().__init__(field=field, to=to, partition_by=partition_by)
        self.cadence = cadence

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        step = parse_timecode(self.cadence)
        last: TemporalRecord | None = None
        last_key: tuple | None = None
        for record in stream:
            if self.to != self.field:
                record = self._ensure_output_field(
                    record, get_field(record, self.field)
                )
            key = partition_key(record, self.partition_by)
            if last is None or last_key != key:
                yield record
                last = record
                last_key = key
                continue

            expect = last.time + step
            while expect < record.time:
                yield clone_record(last, time=expect, **{self.to: None})
                expect = expect + step
            yield record
            last = record
