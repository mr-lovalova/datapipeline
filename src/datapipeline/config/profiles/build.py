from typing import Literal

from pydantic import Field, field_validator

from datapipeline.config.observability import ObservabilityConfig

from .base import Profile, normalize_profile_target

VALID_BUILD_MODES = ("AUTO", "FORCE", "OFF")


def normalize_build_mode(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return "OFF" if value is False else "AUTO"
    name = str(value).strip().upper()
    if name not in VALID_BUILD_MODES:
        raise ValueError(
            f"mode must be one of {', '.join(VALID_BUILD_MODES)}, got {value!r}"
        )
    return name


class BuildProfile(Profile):
    cmd: Literal["build"]
    observability: ObservabilityConfig | None = Field(default=None)
    mode: str | None = Field(default=None)
    target: str

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value):
        return normalize_build_mode(value)

    @field_validator("target", mode="before")
    @classmethod
    def _normalize_target(cls, value):
        return normalize_profile_target(value)


__all__ = ["VALID_BUILD_MODES", "BuildProfile", "normalize_build_mode"]
