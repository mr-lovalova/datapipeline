from typing import Any, Mapping, Optional, Union, List

from pydantic import BaseModel, Field


class BaseRecordConfig(BaseModel):
    """Simplified config used by the record stage and beyond.

    Minimal: stream, id, partition_by; all behavior lives in clauses.
    """

    stream: str
    id: str
    partition_by: Optional[Union[str, list[str]]] = None
    # Record-stage controls (used by 'records' stage and before feature transforms)
    filters: Optional[List[Mapping[str, Any]]] = Field(default=None)
    lag: Optional[str] = Field(default=None)


class FeatureRecordConfig(BaseRecordConfig):
    """Feature-level configuration using simple, explicit fields only."""

    # Record-stage filtering
    filters: Optional[Union[List[Mapping[str, Any]],
                            Mapping[str, Any]]] = Field(default=None)
    # Record-stage transform
    lag: Optional[str] = Field(default=None)
    # Feature-stage transforms
    fill: Optional[Mapping[str, Any]] = Field(default=None)
    scale: Optional[Union[bool, Mapping[str, Any]]] = Field(default=None)
    transforms: Optional[List[Mapping[str, Any]]] = Field(default=None)
    # Sequence/windowing transform (runs last in feature stage)
    sequence: Optional[Mapping[str, Any]] = Field(default=None)

    # Optional tuning
    sort_batch_size: Optional[int] = Field(default=None)
    # Optional record-stage custom transforms (advanced)
    record_transforms: Optional[List[Mapping[str, Any]]] = Field(default=None)
