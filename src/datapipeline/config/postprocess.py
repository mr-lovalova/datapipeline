from typing import Any, List
from pydantic import BaseModel, Field


class PostprocessConfig(BaseModel):
    """Schema for optional postprocess.yaml."""

    transforms: List[Any] | None = Field(default=None)
