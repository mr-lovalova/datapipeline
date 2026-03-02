from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from datapipeline.config.model_utils import normalize_required_text, normalize_string_list


class Task(BaseModel):
    """Concrete executable operation."""

    version: int = Field(default=1)
    kind: Literal["artifact", "runtime"]
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
    kind: Literal["artifact"] = Field(default="artifact")
    output: str

    @model_validator(mode="after")
    def _validate_dependencies(self):
        if self.kind != "artifact":
            raise ValueError("artifact tasks must set kind=artifact")
        if self.id in set(self.dependencies):
            raise ValueError("dependencies must not include the task itself")
        output_path = Path(self.output)
        if output_path.is_absolute():
            raise ValueError("output must be a relative path under artifacts root")
        if ".." in output_path.parts:
            raise ValueError("output must not traverse outside artifacts root")
        return self


class OperationTask(Task):
    kind: Literal["runtime"] = Field(default="runtime")
    runtime_kind: Literal["serve", "inspect"]
    dependencies: list[str] = Field(default_factory=list)
    options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("runtime_kind", mode="before")
    @classmethod
    def _normalize_runtime_kind(cls, value):
        return normalize_required_text(
            value, field_name="runtime_kind", lower=True
        )

    @model_validator(mode="after")
    def _validate_kind(self):
        if self.kind != "runtime":
            raise ValueError("runtime tasks must set kind=runtime")
        return self


__all__ = ["Task", "ArtifactTask", "OperationTask"]
