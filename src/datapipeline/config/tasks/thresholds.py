from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictFloat

from .base import OperationTask


class ThresholdsOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sort: Literal["missing", "nulls"] = "missing"
    threshold: StrictFloat = Field(default=0.95, ge=0.0, le=1.0)


class ThresholdsTask(OperationTask[ThresholdsOptions]):
    entrypoint: Literal["core.runtime.thresholds"] = Field(
        default="core.runtime.thresholds"
    )
    options: ThresholdsOptions = Field(default_factory=ThresholdsOptions)


__all__ = ["ThresholdsOptions", "ThresholdsTask"]
