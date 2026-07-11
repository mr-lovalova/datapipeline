from collections.abc import Mapping
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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
    start_time: datetime | None = None
    end_time: datetime | None = None


class ProjectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    name: str | None = None
    variant: str | None = None
    split: SplitConfig | None = None
    paths: ProjectPaths
    globals: ProjectGlobals = Field(default_factory=ProjectGlobals)

    @model_validator(mode="before")
    @classmethod
    def _reject_globals_split(cls, value: object) -> object:
        if isinstance(value, Mapping):
            globals_config = value.get("globals")
            if isinstance(globals_config, Mapping) and "split" in globals_config:
                raise ValueError(
                    "globals.split is not supported; define top-level project.split"
                )
        return value
