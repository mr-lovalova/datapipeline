from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator


ProjectPath = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class ProjectPaths(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ingests: ProjectPath | list[ProjectPath]
    streams: ProjectPath | list[ProjectPath]
    sources: ProjectPath | list[ProjectPath]
    dataset: ProjectPath
    artifacts: ProjectPath
    operations: ProjectPath | None = None
    profiles: ProjectPath | None = None

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
    artifact_revision: int = Field(strict=True, gt=0)
    name: str | None = None
    variant: str | None = None
    paths: ProjectPaths
    globals: ProjectGlobals = Field(default_factory=ProjectGlobals)
