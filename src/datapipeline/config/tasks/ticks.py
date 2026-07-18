from typing import Annotated

from pydantic import Field, StringConstraints, field_validator

from .base import ArtifactTask


FieldName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class TicksTask(ArtifactTask):
    entrypoint: str = Field(default="core.artifact.ticks")
    stream: FieldName
    grid_by: list[FieldName] = Field(default_factory=list)

    @field_validator("grid_by")
    @classmethod
    def _validate_grid_by(cls, grid_by: list[str]) -> list[str]:
        if len(grid_by) != len(set(grid_by)):
            raise ValueError("grid_by must not contain duplicate fields")
        if "time" in grid_by:
            raise ValueError("grid_by must not contain the reserved field 'time'")
        return grid_by
