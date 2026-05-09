from dataclasses import dataclass
from datetime import datetime

from datapipeline.domain.record import TemporalRecord


@dataclass(frozen=True)
class JoinedRow:
    time: datetime
    values: dict[str, TemporalRecord | None]
