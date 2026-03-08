from typing import Literal

from pydantic import Field, field_validator

from datapipeline.config.observability import ObservabilityConfig

from .base import Profile, normalize_profile_target
from .output import ServeOutputConfig
from .runtime_build import RuntimeBuildConfig


class InspectProfile(Profile):
    cmd: Literal["inspect"]
    target: str
    output: ServeOutputConfig | None = None
    observability: ObservabilityConfig | None = Field(default=None)
    build: RuntimeBuildConfig | None = Field(default=None)

    @field_validator("target", mode="before")
    @classmethod
    def _normalize_target(cls, value):
        return normalize_profile_target(value)

__all__ = ["InspectProfile"]
