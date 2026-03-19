from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from datapipeline.config.observability import ObservabilityConfig

from .build import normalize_build_mode
from .output import ServeOutputConfig
from .runtime_build import RuntimeBuildConfig


class ProfileDefaults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cmd: str
    source_path: Path | None = Field(default=None, exclude=True)


class ServeProfileDefaults(ProfileDefaults):
    cmd: Literal["serve"]
    cache: bool | None = None
    output: ServeOutputConfig | None = None
    observability: ObservabilityConfig | None = Field(default=None)
    build: RuntimeBuildConfig | None = Field(default=None)
    keep: str | None = Field(default=None, min_length=1)
    limit: int | None = Field(default=None, ge=1)
    stage: int | None = Field(default=None, ge=0)
    throttle_ms: float | None = Field(default=None, ge=0.0)


class BuildProfileDefaults(ProfileDefaults):
    cmd: Literal["build"]
    observability: ObservabilityConfig | None = Field(default=None)
    mode: str | None = Field(default=None)

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value):
        return normalize_build_mode(value)


class InspectProfileDefaults(ProfileDefaults):
    cmd: Literal["inspect"]
    cache: bool | None = None
    output: ServeOutputConfig | None = None
    observability: ObservabilityConfig | None = Field(default=None)
    build: RuntimeBuildConfig | None = Field(default=None)


__all__ = [
    "ProfileDefaults",
    "ServeProfileDefaults",
    "BuildProfileDefaults",
    "InspectProfileDefaults",
]
