from typing import Union, List, Any
from datetime import datetime

from datapipeline.config.dataset.normalize import floor_time_to_bucket
from datapipeline.transforms.vector_utils import PARTITION_SEP


class FeatureIdGenerator:
    """
    Generates unique feature keys by appending suffixes from expand_by fields.
    """

    COMPONENT_PREFIX = "@"
    COMPONENT_JOINER = "_"
    VALUE_DELIMITER = ":"

    def __init__(self, partition_by: Union[str, List[str], None]):
        self.partition_by = partition_by

    def _format_component(self, field: str, value: Any) -> str:
        value_str = "" if value is None else str(value)
        return f"{self.COMPONENT_PREFIX}{field}{self.VALUE_DELIMITER}{value_str}"

    def generate(self, base_id: str, record: Any) -> str:
        if not self.partition_by:
            return base_id
        if isinstance(self.partition_by, str):
            value = getattr(record, self.partition_by)
            suffix = self._format_component(self.partition_by, value)
        else:
            parts = [
                self._format_component(field, getattr(record, field))
                for field in self.partition_by
            ]
            suffix = self.COMPONENT_JOINER.join(parts)
        return f"{base_id}{PARTITION_SEP}{suffix}"


def _anchor_time(item: Any) -> datetime | None:
    """Return representative datetime for grouping.

    - FeatureRecord -> record.time
    - FeatureRecordSequence -> first record time if present
    """
    rec = getattr(item, "record", None)
    if rec is not None:
        return getattr(rec, "time", None)
    recs = getattr(item, "records", None)
    return getattr(recs[0], "time", None) if recs else None


def group_key_for(item: Any, cadence: str) -> tuple:
    """Compute 1-tuple bucket key from a FeatureRecord or FeatureRecordSequence."""
    t = _anchor_time(item)
    return (floor_time_to_bucket(t, cadence),)
