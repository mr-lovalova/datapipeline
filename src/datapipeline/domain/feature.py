from datapipeline.domain.record import TimeSeriesRecord
from dataclasses import dataclass
from typing import Union


@dataclass
class FeatureRecord:
    record: Union[TimeSeriesRecord, list[TimeSeriesRecord]]
    feature_id: str
    group_key: tuple
