from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class TimeSeriesRecord:
    """Canonical time-series payload used throughout the pipeline."""

    time: datetime
    value: Any

    def __post_init__(self) -> None:
        if self.time.tzinfo is None:
            raise ValueError("time must be timezone-aware")
        self.time = self.time.astimezone(timezone.utc)

