from datapipeline.domain.record import TemporalRecord
from dataclasses import dataclass
from typing import Any


@dataclass
class BaseFeature:
    id: str


@dataclass
class FeatureRecord(BaseFeature):
    record: TemporalRecord
    value: Any


@dataclass
class FeatureRecordSequence(BaseFeature):
    records: list[TemporalRecord]
    values: list[Any]
