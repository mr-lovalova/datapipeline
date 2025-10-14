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


def group_bucket(t: datetime, resolution: str) -> datetime:
    """Return the bucketed timestamp for the given resolution (time-only)."""
    return floor_time_to_resolution(t, resolution)
