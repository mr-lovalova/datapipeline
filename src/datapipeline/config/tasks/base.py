from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from datapipeline.config.model_utils import normalize_required_text


class Task(BaseModel):
    """Concrete executable operation."""

    model_config = ConfigDict(extra="forbid")

    version: int = Field(default=1)
    kind: Literal["artifact", "runtime"]
    id: str
    source_path: Path | None = Field(default=None, exclude=True)
    entrypoint: str

    @field_validator("id", mode="before")
    @classmethod
    def _normalize_id(cls, value):
        return normalize_required_text(value, field_name="id", lower=True)

    @field_validator("entrypoint", mode="before")
    @classmethod
    def _normalize_entrypoint(cls, value):
        return normalize_required_text(value, field_name="entrypoint")

class ArtifactTask(Task):
    kind: Literal["artifact"] = Field(default="artifact")
    output: str

    @model_validator(mode="after")
    def _validate_output(self):
        output_path = Path(self.output)
        if output_path.is_absolute():
            raise ValueError("output must be a relative path under artifacts root")
        if ".." in output_path.parts:
            raise ValueError("output must not traverse outside artifacts root")
        return self


class OperationTask(Task):
    kind: Literal["runtime"] = Field(default="runtime")
    options: dict[str, Any] = Field(default_factory=dict)


__all__ = ["Task", "ArtifactTask", "OperationTask"]
