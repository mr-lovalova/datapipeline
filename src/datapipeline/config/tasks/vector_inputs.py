from typing import Literal

from pydantic import Field

from .base import ArtifactTask


class VectorInputsTask(ArtifactTask):
    id: Literal["vector_inputs"] = Field(default="vector_inputs")
    entrypoint: str = Field(default="core.artifact.vector_inputs")
    output: str = Field(default="build/vector_inputs/manifest.json")
