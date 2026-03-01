from typing import Literal

from pydantic import Field, field_validator

from datapipeline.config.model_utils import normalize_required_text
from datapipeline.config.observability import ObservabilityConfig

from .base import Profile
from .output import ServeOutputConfig
from .runtime_build import RuntimeBuildConfig


class ServeProfile(Profile):
    type: Literal["serve"]
    target: str
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
        return normalize_required_text(value, field_name="target")


__all__ = ["ServeProfile"]
