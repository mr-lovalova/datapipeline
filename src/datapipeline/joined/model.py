from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from datapipeline.domain.record import TemporalRecord

JoinRecord = TemporalRecord | Mapping[str, object]


@dataclass(frozen=True)
class JoinedRow:
    time: datetime
    values: dict[str, JoinRecord | None]
