from typing import Literal

from pydantic import Field, field_validator

from datapipeline.config.observability import ObservabilityConfig

from .base import Profile, normalize_profile_target
from .build import normalize_artifact_mode
from .output import ServeOutputConfig


class InspectProfile(Profile):
    cmd: Literal["inspect"]
    target: str
    output: ServeOutputConfig | None = None
    observability: ObservabilityConfig | None = Field(default=None)
    artifact_mode: str | None = Field(default=None)

    @field_validator("target", mode="before")
    @classmethod
    def _normalize_target(cls, value: object) -> str:
        return normalize_profile_target(value)

    @field_validator("artifact_mode", mode="before")
    @classmethod
    def _normalize_artifact_mode(cls, value: object) -> str | None:
        return normalize_artifact_mode(value)


__all__ = ["InspectProfile"]
