from typing import Literal

from pydantic import Field, field_validator

from datapipeline.config.observability import ObservabilityConfig

from .base import Profile, normalize_profile_operation
from .output import ServeOutputConfig


class InspectProfile(Profile):
    cmd: Literal["inspect"]
    operation: str
    output: ServeOutputConfig | None = None
    observability: ObservabilityConfig | None = Field(default=None)

    @field_validator("operation", mode="before")
    @classmethod
    def _normalize_operation(cls, value: object) -> str:
        return normalize_profile_operation(value)
