from typing import List
from pydantic import BaseModel, Field
from datapipeline.config.dataset.group_by import GroupBy
from datapipeline.config.dataset.feature import BaseRecordConfig, FeatureRecordConfig


class RecordDatasetConfig(BaseModel):
    features: List[BaseRecordConfig] = Field(default_factory=list)
    targets:  List[BaseRecordConfig] = Field(default_factory=list)


class FeatureDatasetConfig(BaseModel):
    group_by: GroupBy
    features: List[FeatureRecordConfig] = Field(default_factory=list)
    targets:  List[FeatureRecordConfig] = Field(default_factory=list)


class VectorDatasetConfig(FeatureDatasetConfig):
    pass
