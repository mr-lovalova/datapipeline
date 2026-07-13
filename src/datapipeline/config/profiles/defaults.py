from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from datapipeline.config.execution import ExecutionConfig
from datapipeline.config.observability import ObservabilityConfig
from datapipeline.config.preview import PreviewStage

from .build import normalize_artifact_mode
from .output import ServeOutputConfig


class ProfileDefaults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cmd: str
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    observability: ObservabilityConfig | None = None
    source_path: Path | None = Field(default=None, exclude=True)


class ServeProfileDefaults(ProfileDefaults):
    cmd: Literal["serve"]
    output: ServeOutputConfig | None = None
    artifact_mode: str | None = Field(default=None)
    limit: int | None = Field(default=None, ge=1)
    preview: PreviewStage | None = None
    throttle_ms: float | None = Field(default=None, ge=0.0)

    @field_validator("artifact_mode", mode="before")
    @classmethod
    def _normalize_artifact_mode(cls, value: object) -> str | None:
        return normalize_artifact_mode(value)


class BuildProfileDefaults(ProfileDefaults):
    cmd: Literal["build"]
    mode: str | None = Field(default=None)

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value: object) -> str | None:
        return normalize_artifact_mode(value)


class InspectProfileDefaults(ProfileDefaults):
    cmd: Literal["inspect"]
    output: ServeOutputConfig | None = None
    artifact_mode: str | None = Field(default=None)

    @field_validator("artifact_mode", mode="before")
    @classmethod
    def _normalize_artifact_mode(cls, value: object) -> str | None:
        return normalize_artifact_mode(value)


class MaterializeProfileDefaults(ProfileDefaults):
    cmd: Literal["materialize"]
    artifact_mode: str | None = Field(default=None)
    overwrite: StrictBool | None = None

    @field_validator("artifact_mode", mode="before")
    @classmethod
    def _normalize_artifact_mode(cls, value: object) -> str | None:
        return normalize_artifact_mode(value)


__all__ = [
    "ProfileDefaults",
    "ServeProfileDefaults",
    "BuildProfileDefaults",
    "InspectProfileDefaults",
    "MaterializeProfileDefaults",
]
