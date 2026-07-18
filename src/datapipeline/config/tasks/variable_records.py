from typing import Literal

from pydantic import Field

from .base import ArtifactTask


class VariableRecordsTask(ArtifactTask):
    id: Literal["variable_records"] = Field(default="variable_records")
    entrypoint: str = Field(default="core.artifact.variable_records")
    output: str = Field(default="build/variable_records/manifest.json")
