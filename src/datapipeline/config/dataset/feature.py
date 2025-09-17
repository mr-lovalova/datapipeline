"""Dataset feature configuration models."""

from __future__ import annotations

from typing import Any, List, Optional, Union

from pydantic import BaseModel


class BaseRecordConfig(BaseModel):
    """Configuration shared by record- and feature-level pipelines."""

    stream: str
    feature_id: str
    partition_by: Optional[Union[str, List[str]]] = None
    filters: Optional[List[Any]] = None
    transforms: Optional[List[Any]] = None


class FeatureRecordConfig(BaseRecordConfig):
    """Configuration for feature-level pipelines (post record stage)."""

    feature_transforms: Optional[List[Any]] = None
    sequence_transforms: Optional[List[Any]] = None
