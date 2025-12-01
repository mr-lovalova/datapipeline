from __future__ import annotations

from datetime import datetime
from typing import Optional, Literal
from pydantic import BaseModel, Field, ConfigDict, field_validator

from datapipeline.config.split import SplitConfig


class ProjectPaths(BaseModel):
    streams: str
    sources: str
    dataset: str
    postprocess: str
    artifacts: str
    tasks: str | None = None


class ProjectGlobals(BaseModel):
    model_config = ConfigDict(extra='allow')
    start_time: str | datetime = Field(
        default="auto",
        description="Start of the dataset window: 'auto' to resolve from stats using window_mode, or an ISO datetime.",
    )
    end_time: str | datetime = Field(
        default="auto",
        description="End of the dataset window: 'auto' to resolve from stats using window_mode, or an ISO datetime.",
    )
    # Optional dataset split configuration (typed). Accepts mapping or string.
    split: Optional[SplitConfig] = None

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def _normalize_time(cls, value):
        if value is None:
            return "auto"
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if text.lower() == "auto":
            return "auto"
        return text

    # No coercion or discriminator injection; default behavior:
    # - If 'split' omitted or null -> disabled
    # - If mapping lacks 'mode' -> validated as HashSplitConfig (first in union)


class ProjectConfig(BaseModel):
    version: int = 1
    name: str | None = None
    paths: ProjectPaths
    globals: ProjectGlobals = Field(default_factory=ProjectGlobals)
