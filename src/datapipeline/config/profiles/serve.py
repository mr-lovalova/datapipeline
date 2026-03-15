from typing import Literal

from pydantic import Field, field_validator

from datapipeline.config.observability import ObservabilityConfig

from .base import Profile, normalize_profile_target
from .output import ServeOutputConfig
from .runtime_build import RuntimeBuildConfig


class ServeProfile(Profile):
    cmd: Literal["serve"]
    target: str
    cache: bool | None = None
    output: ServeOutputConfig | None = None
    observability: ObservabilityConfig | None = Field(default=None)
    build: RuntimeBuildConfig | None = Field(default=None)
    keep: str | None = Field(default=None, min_length=1)
    limit: int | None = Field(default=None, ge=1)
    stage: int | None = Field(default=None, ge=0)
    throttle_ms: float | None = Field(default=None, ge=0.0)

    @field_validator("target", mode="before")
    @classmethod
    def _normalize_target(cls, value):
        return normalize_profile_target(value)

__all__ = ["ServeProfile"]
