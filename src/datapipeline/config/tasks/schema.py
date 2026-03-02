from typing import Literal

from pydantic import Field

from .base import ArtifactTask


class SchemaTask(ArtifactTask):
    id: Literal["schema"] = Field(default="schema")
    entrypoint: str = Field(default="core.artifact.schema")
    output: str = Field(default="build/schema.json")
    cadence_strategy: Literal["max"] = Field(default="max")


__all__ = ["SchemaTask"]
