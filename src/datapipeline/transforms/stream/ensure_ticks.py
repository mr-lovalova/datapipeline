from typing import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.interfaces import PartitionedFieldStreamTransformBase
from datapipeline.transforms.utils import clone_record
from datapipeline.utils.time import parse_timecode


class EnsureCadenceTransform(PartitionedFieldStreamTransformBase):
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
        partition_by: str | list[str] | None = None,
    ) -> None:
        super().__init__(field=field, partition_by=partition_by)
        self.cadence = cadence

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        step = parse_timecode(self.cadence)
        last: TemporalRecord | None = None
        last_key: tuple | None = None
        for record in stream:
            key = self.partition_key(record)
            if last is None or last_key != key:
                yield record
                last = record
                last_key = key
                continue

            expect = last.time + step
            while expect < record.time:
                yield self._placeholder_record(last, expect)
                expect = expect + step
            yield record
            last = record

    def _placeholder_record(self, record: TemporalRecord, time) -> TemporalRecord:
        keep = {"time", self.field, *self.partition_fields()}
        updates = {
            key: None
            for key in record.__dict__
            if not key.startswith("_") and key not in keep
        }
        return clone_record(record, time=time, **updates, **{self.field: None})
