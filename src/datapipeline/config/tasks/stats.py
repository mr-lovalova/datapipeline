from typing import Literal

from pydantic import Field

from .base import ArtifactTask


class StatsTask(ArtifactTask):
    id: Literal["stats"] = Field(default="stats")
    entrypoint: str = Field(default="core.build.stats")
    output: str = Field(default="build/stats.json")
    mode: Literal["final", "raw"]


__all__ = ["StatsTask"]
