from datapipeline.domain.record import TemporalRecord
from dataclasses import dataclass


@dataclass
class BaseFeature:
    id: str


@dataclass
class FeatureRecord(BaseFeature):
    record: TemporalRecord


@dataclass
class FeatureRecordSequence(BaseFeature):
    records: list[TemporalRecord]
