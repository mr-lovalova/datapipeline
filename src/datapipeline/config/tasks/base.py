from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from datapipeline.config.model_utils import normalize_required_text, normalize_string_list


class Task(BaseModel):
    """Concrete executable operation."""

    version: int = Field(default=1)
    id: str
    source_path: Path | None = Field(default=None, exclude=True)
    entrypoint: str
    dependencies: list[str] = Field(default_factory=list)

    @field_validator("id", mode="before")
    @classmethod
    def _normalize_id(cls, value):
        return normalize_required_text(value, field_name="id", lower=True)

    @field_validator("entrypoint", mode="before")
    @classmethod
    def _normalize_entrypoint(cls, value):
        return normalize_required_text(value, field_name="entrypoint")

    @field_validator("dependencies", mode="before")
    @classmethod
    def _normalize_dependencies(cls, value):
        return normalize_string_list(
            value,
            field_name="dependencies",
            lower=True,
        )


class ArtifactTask(Task):
    output: str

    @model_validator(mode="after")
    def _validate_dependencies(self):
        if self.id in set(self.dependencies):
            raise ValueError("dependencies must not include the task itself")
        return self


class OperationTask(Task):
    dependencies: list[str] = Field(default_factory=list)
    options: dict[str, Any] = Field(default_factory=dict)


__all__ = ["Task", "ArtifactTask", "OperationTask"]
