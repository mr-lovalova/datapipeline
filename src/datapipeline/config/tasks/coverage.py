from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictFloat

from .base import OperationTask


class CoverageOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sort: Literal["missing", "nulls"] = "missing"
    threshold: StrictFloat = Field(default=0.95, ge=0.0, le=1.0)


class CoverageTask(OperationTask[CoverageOptions]):
    entrypoint: Literal["core.runtime.coverage"] = Field(
        default="core.runtime.coverage"
    )
    options: CoverageOptions = Field(default_factory=CoverageOptions)


__all__ = ["CoverageOptions", "CoverageTask"]
