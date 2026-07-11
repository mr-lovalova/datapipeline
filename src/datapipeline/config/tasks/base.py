from collections.abc import Mapping
from pathlib import Path
from typing import Generic, Literal, TypeVar

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


OperationOptionsT = TypeVar("OperationOptionsT")


class OperationTask(Task, Generic[OperationOptionsT]):
    kind: Literal["runtime"] = Field(default="runtime")
    requires: tuple[str, ...] = ()
    # Bare OperationTask is the plugin fallback; concrete tasks replace this
    # default with their own option type.
    options: OperationOptionsT = Field(default_factory=dict)  # type: ignore[assignment]

    @field_validator("requires", mode="before")
    @classmethod
    def _normalize_requires(cls, value: object) -> tuple[str, ...]:
        if value is None:
            return ()
        if not isinstance(value, (list, tuple)):
            raise ValueError("requires must be a list of artifact task ids")
        requires = tuple(
            normalize_required_text(item, field_name="requires item", lower=True)
            for item in value
        )
        if len(requires) != len(set(requires)):
            raise ValueError("requires must not contain duplicate artifact task ids")
        return requires

    @field_validator("options", mode="before")
    @classmethod
    def _require_options_mapping(cls, value):
        if isinstance(value, (Mapping, BaseModel)):
            return value
        raise ValueError("options must be a mapping")


__all__ = ["Task", "ArtifactTask", "OperationTask"]
