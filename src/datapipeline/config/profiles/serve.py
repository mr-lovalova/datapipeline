from typing import Literal

from pydantic import Field, field_validator

from datapipeline.config.observability import ObservabilityConfig
from datapipeline.config.preview import PreviewStage

from .base import Profile, normalize_profile_operation
from .output import ServeOutputConfig


def normalize_include_outputs(value: object) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("include_outputs must be a list of dataset output ids")
    output_ids: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise ValueError("include_outputs entries must be strings")
        if not item.strip():
            raise ValueError("include_outputs entries must not be empty")
        if item != item.strip():
            raise ValueError(
                "include_outputs entries must not contain outer whitespace"
            )
        if item in seen:
            raise ValueError(f"duplicate dataset output id {item!r}")
        output_ids.append(item)
        seen.add(item)
    return output_ids


class ServeProfile(Profile):
    cmd: Literal["serve"]
    operation: str
    output: ServeOutputConfig | None = None
    observability: ObservabilityConfig | None = Field(default=None)
    include_outputs: list[str] | None = Field(default=None, min_length=1)
    limit: int | None = Field(default=None, ge=1)
    preview: PreviewStage | None = None
    throttle_ms: float | None = Field(default=None, ge=0.0, allow_inf_nan=False)

    @field_validator("operation", mode="before")
    @classmethod
    def _normalize_operation(cls, value: object) -> str:
        return normalize_profile_operation(value)

    @field_validator("include_outputs", mode="before")
    @classmethod
    def _normalize_include_outputs(cls, value: object) -> list[str] | None:
        return normalize_include_outputs(value)
