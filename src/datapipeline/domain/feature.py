from dataclasses import dataclass
from datetime import datetime
from typing import Any

from datapipeline.domain.record import TemporalRecord


@dataclass
class BaseFeature:
    id: str


@dataclass
class FeatureRecord(BaseFeature):
    record: TemporalRecord
    value: Any
    entity_key: tuple = ()

    @property
    def time(self) -> datetime:
        return self.record.time


@dataclass
class FeatureSequence(BaseFeature):
    time: datetime
    values: list[Any]
    entity_key: tuple = ()
