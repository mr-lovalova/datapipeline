from typing import Literal

from pydantic import Field, field_validator, model_validator

from datapipeline.config.observability import ObservabilityConfig

from .base import Profile, normalize_profile_target
from .output import ServeOutputConfig
from .runtime_build import RuntimeBuildConfig


class ServeProfile(Profile):
    cmd: Literal["serve"]
    target: str
    output: ServeOutputConfig | None = None
    observability: ObservabilityConfig | None = Field(default=None)
    build: RuntimeBuildConfig | None = Field(default=None)
    keep: str | None = Field(default=None, min_length=1)
    splits: list[str] | None = Field(default=None, min_length=1)
    limit: int | None = Field(default=None, ge=1)
    preview_index: int | None = Field(default=None, ge=0)
    throttle_ms: float | None = Field(default=None, ge=0.0)

    @field_validator("target", mode="before")
    @classmethod
    def _normalize_target(cls, value):
        return normalize_profile_target(value)

    @field_validator("splits", mode="before")
    @classmethod
    def _normalize_splits(cls, value):
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError("splits must be a list of split labels")
        labels: list[str] = []
        seen: set[str] = set()
        for item in value:
            label = str(item).strip()
            if not label:
                raise ValueError("splits labels must not be empty")
            if label in seen:
                raise ValueError(f"duplicate split label {label!r}")
            labels.append(label)
            seen.add(label)
        return labels

    @model_validator(mode="after")
    def _validate_split_selection(self):
        if self.keep is not None and self.splits is not None:
            raise ValueError("serve profile cannot define both keep and splits")
        return self

__all__ = ["ServeProfile"]
