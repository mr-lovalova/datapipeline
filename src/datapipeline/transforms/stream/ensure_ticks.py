from typing import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.feature import FeatureRecord
from datapipeline.utils.time import parse_timecode


def ensure_ticks(stream: Iterator[FeatureRecord], tick: str) -> Iterator[FeatureRecord]:
    """Insert placeholder FeatureRecords so timestamps are exactly one tick apart per feature id.

    - tick: duration string (e.g., "10m", "1h", "30s").
    - Placeholders carry value=None using a new TemporalRecord; id and group_key are preserved.
    - Assumes input sorted by (feature_id, record.time).
    """
    step = parse_timecode(tick)
    last: FeatureRecord | None = None
    for fr in stream:
        if (last is None) or (last.id != fr.id):
            yield fr
            last = fr
            continue

        expect = last.record.time + step
        while expect < fr.record.time:
            # Derive a group_key consistent with the placeholder time. We assume
            # the first element of group_key corresponds to the time key (the
            # current GroupBy only supports time keys). If not present, fall
            # back to a single-element tuple with the expected time.
            if isinstance(fr.group_key, tuple) and len(fr.group_key) > 0:
                gk = list(fr.group_key)
                gk[0] = expect
                group_key = tuple(gk)
            else:
                group_key = (expect,)

            yield FeatureRecord(
                record=TemporalRecord(time=expect, value=None),
                id=fr.id,
                group_key=group_key,
            )
            expect = expect + step
        yield fr
        last = fr
