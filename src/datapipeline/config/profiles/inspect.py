from typing import Literal

from pydantic import Field, field_validator

from datapipeline.config.model_utils import normalize_required_text
from datapipeline.config.observability import ObservabilityConfig

from .base import Profile
from .output import ServeOutputConfig


class InspectProfile(Profile):
    type: Literal["inspect"]
    target: str
    output: ServeOutputConfig | None = None
    observability: ObservabilityConfig | None = Field(default=None)

    @field_validator("target", mode="before")
    @classmethod
    def _normalize_target(cls, value):
        return normalize_required_text(value, field_name="target")


__all__ = ["InspectProfile"]
