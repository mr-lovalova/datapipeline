from typing import Iterator

from datapipeline.domain.feature import FeatureRecord
from datapipeline.utils.time import parse_timecode
from datapipeline.transforms.interfaces import FieldStreamTransformBase
from datapipeline.transforms.utils import clone_record


class EnsureCadenceTransform(FieldStreamTransformBase):
    """Insert placeholder FeatureRecords so timestamps are exactly one cadence apart per feature id.

    - cadence: duration string (e.g., "10m", "1h", "30s").
    - Placeholders carry value=None and inherit the feature id; group bucketing
      is applied later at vector assembly from record.time.
    - Assumes input sorted by (feature_id, record.time).
    """

    def __init__(self, cadence: str, field: str = "value", to: str | None = None) -> None:
        super().__init__(field=field, to=to)
        self.cadence = cadence

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        step = parse_timecode(self.cadence)
        last: FeatureRecord | None = None
        for fr in stream:
            if (last is None) or (last.id != fr.id):
                yield fr
                last = fr
                continue

            expect = last.record.time + step
            while expect < fr.record.time:
                yield FeatureRecord(
                    record=clone_record(last.record, time=expect, **{self.to: None}),
                    id=fr.id,
                )
                expect = expect + step
            yield fr
            last = fr

