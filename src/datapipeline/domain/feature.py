from datapipeline.domain.record import TimeSeriesRecord
from dataclasses import dataclass


@dataclass
class BaseFeature:
    feature_id: str
    group_key: tuple


@dataclass
class FeatureRecord(BaseFeature):
    record: TimeSeriesRecord


@dataclass
class FeatureSequence(BaseFeature):
    records: list[TimeSeriesRecord]
