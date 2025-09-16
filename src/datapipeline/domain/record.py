
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Record:
    # Canonical payload for pipelines; may be numeric or categorical.
    value: Any


@dataclass
class TimeFeatureRecord(Record):
    time: datetime

    def __post_init__(self):
        if self.time.tzinfo is None:
            raise ValueError("TimeFeatureRecord.time must be timezone-aware")
        self.time = self.time.astimezone(timezone.utc)
