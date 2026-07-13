from pydantic import Field

from .base import ArtifactTask


class TicksTask(ArtifactTask):
    entrypoint: str = Field(default="core.artifact.ticks")
    stream: str
    grid_by: list[str] = Field(default_factory=list)


__all__ = ["TicksTask"]
