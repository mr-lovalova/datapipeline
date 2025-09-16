from pydantic import BaseModel
from typing import Optional, List, Any, Union


class BaseRecordConfig(BaseModel):
    stream: str
    feature_id: str
    partition_by: Optional[Union[str, List[str]]] = None
    filters:            list[Any] = None
    transforms:         list[Any] = None


class FeatureRecordConfig(BaseRecordConfig):
    feature_transforms: list[Any] = None
    sequence_transforms: list[Any] = None
