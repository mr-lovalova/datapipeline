from typing import Literal

from pydantic import Field

from .base import ArtifactTask


class MetadataTask(ArtifactTask):
    id: Literal["metadata"] = Field(default="metadata")
    entrypoint: str = Field(default="core.artifact.metadata")
    output: str = Field(default="build/metadata.json")
    dependencies: list[str] = Field(default_factory=list)
    cadence_strategy: Literal["max"] = Field(default="max")
    window_mode: Literal["union", "intersection", "strict", "relaxed"] = Field(
        default="intersection"
    )


__all__ = ["MetadataTask"]
