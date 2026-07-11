from typing import Literal

from pydantic import Field, field_validator

from datapipeline.config.observability import ObservabilityConfig

from .base import Profile, normalize_profile_target

ARTIFACT_MODES = ("AUTO", "FORCE", "OFF")


def normalize_artifact_mode(value: object) -> str | None:
    if value is None:
        return None
    name = str(value).strip().upper()
    if name not in ARTIFACT_MODES:
        raise ValueError(
            f"artifact mode must be one of {', '.join(ARTIFACT_MODES)}, got {value!r}"
        )
    return name


class BuildProfile(Profile):
    cmd: Literal["build"]
    observability: ObservabilityConfig | None = Field(default=None)
    mode: str | None = Field(default=None)
    target: str

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value: object) -> str | None:
        return normalize_artifact_mode(value)

    @field_validator("target", mode="before")
    @classmethod
    def _normalize_target(cls, value: object) -> str:
        return normalize_profile_target(value)


__all__ = ["ARTIFACT_MODES", "BuildProfile", "normalize_artifact_mode"]
