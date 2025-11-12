from typing import Any, Mapping, Optional, Union

from pydantic import BaseModel, Field


class BaseRecordConfig(BaseModel):
    record_stream: str


class FeatureRecordConfig(BaseRecordConfig):
    id: str
    scale: Optional[Union[bool, Mapping[str, Any]]] = Field(default=False)
    sequence: Optional[Mapping[str, Any]] = Field(default=None)
