import warnings
from datetime import datetime
from typing import Literal, Optional

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
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    # Deprecated: use top-level project.split instead.
    split: Optional[SplitConfig] = None


class ProjectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    name: str | None = None
    variant: str | None = None
    split: Optional[SplitConfig] = None
    paths: ProjectPaths
    globals: ProjectGlobals = Field(default_factory=ProjectGlobals)

    @model_validator(mode="after")
    def _validate_split_location(self):
        if self.split is not None and self.globals.split is not None:
            raise ValueError(
                "project split must be defined either as top-level 'split' "
                "or as deprecated 'globals.split', not both"
            )
        if self.globals.split is not None:
            warnings.warn(
                "project globals.split is deprecated; move split to top-level "
                "project.split",
                FutureWarning,
                stacklevel=2,
            )
        return self

    @property
    def resolved_split(self) -> Optional[SplitConfig]:
        if self.split is not None:
            return self.split
        return self.globals.split
