from typing import Literal

from pydantic import Field

from .base import ArtifactTask


class ScalerTask(ArtifactTask):
    id: Literal["scaler"] = Field(default="scaler")
    entrypoint: str = Field(default="core.artifact.scaler")
    output: str = Field(default="build/scaler.json")
    split_label: str = Field(default="train")


__all__ = ["ScalerTask"]
