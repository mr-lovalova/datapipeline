from datapipeline.domain.record import Record
from dataclasses import dataclass
from typing import Union


@dataclass
class FeatureRecord:
    record: Union[Record, list[Record]]
    feature_id: str
    group_key: tuple
