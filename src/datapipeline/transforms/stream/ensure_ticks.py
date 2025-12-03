from typing import Iterator

from dataclasses import replace

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.utils.time import parse_timecode


def ensure_cadence(stream: Iterator[FeatureRecord], cadence: str) -> Iterator[FeatureRecord]:
    """Insert placeholder FeatureRecords so timestamps are exactly one cadence apart per feature id.

    - cadence: duration string (e.g., "10m", "1h", "30s").
    - Placeholders carry value=None and inherit the feature id; group bucketing
      is applied later at vector assembly from record.time.
    - Assumes input sorted by (feature_id, record.time).
    """
    step = parse_timecode(cadence)
    last: FeatureRecord | None = None
    for fr in stream:
        if (last is None) or (last.id != fr.id):
            yield fr
            last = fr
            continue

        expect = last.record.time + step
        while expect < fr.record.time:
            yield FeatureRecord(
                record=replace(last.record, time=expect, value=None),
                id=fr.id,
            )
            expect = expect + step
        yield fr
        last = fr
