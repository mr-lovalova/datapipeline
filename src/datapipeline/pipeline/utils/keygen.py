from typing import Union, List, Any
from datetime import datetime

from datapipeline.config.dataset.normalize import floor_time_to_resolution


class FeatureIdGenerator:
    """
    Generates unique feature keys by appending suffixes from expand_by fields.
    """

    def __init__(self, partition_by: Union[str, List[str], None]):
        self.partition_by = partition_by

    def generate(self, base_id: str, record: Any) -> str:
        if not self.partition_by:
            return base_id
        if isinstance(self.partition_by, str):
            suffix = getattr(record, self.partition_by)
        else:
            suffix = "__".join(str(getattr(record, f))
                               for f in self.partition_by)
        return f"{base_id}__{suffix}"


def _anchor_time(item: Any) -> datetime | None:
    """Return representative datetime for grouping.

    - FeatureRecord → record.time
    - FeatureRecordSequence → first record time if present
    """
    rec = getattr(item, "record", None)
    if rec is not None:
        return getattr(rec, "time", None)
    recs = getattr(item, "records", None)
    return getattr(recs[0], "time", None) if recs else None


def group_key_for(item: Any, resolution: str) -> tuple:
    """Compute 1-tuple bucket key from a FeatureRecord or FeatureRecordSequence."""
    t = _anchor_time(item)
    return (floor_time_to_resolution(t, resolution),)
