from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from datapipeline.config.split import SplitConfig


class ProjectPaths(BaseModel):
    ingests: str | list[str]
    streams: str | list[str]
    sources: str | list[str]
    dataset: str
    postprocess: str
    artifacts: str
    tasks: str | None = None
    profiles: str | None = None

    @field_validator("ingests", "streams", "sources")
    @classmethod
    def require_config_roots(cls, value: str | list[str]) -> str | list[str]:
        if isinstance(value, list) and not value:
            raise ValueError("project path lists must not be empty")
        return value


class ProjectGlobals(BaseModel):
    model_config = ConfigDict(extra="allow")
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    # Optional dataset split configuration (typed). Accepts mapping or string.
    split: Optional[SplitConfig] = None

    # No coercion or discriminator injection; default behavior:
    # - If 'split' omitted or null -> disabled
    # - If mapping lacks 'mode' -> validated as HashSplitConfig (first in union)


class ProjectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    name: str | None = None
    variant: str | None = None
    paths: ProjectPaths
    globals: ProjectGlobals = Field(default_factory=ProjectGlobals)
