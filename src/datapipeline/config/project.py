from __future__ import annotations

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class ProjectPaths(BaseModel):
    streams: str
    sources: str
    dataset: str


class ProjectGlobals(BaseModel):
    model_config = ConfigDict(extra='allow')

    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class ProjectConfig(BaseModel):
    version: int = 1
    paths: ProjectPaths
    globals: ProjectGlobals = Field(default_factory=ProjectGlobals)
