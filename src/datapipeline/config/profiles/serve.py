from typing import Literal

from pydantic import Field, field_validator

from datapipeline.config.observability import ObservabilityConfig
from datapipeline.config.preview import PreviewStage

from .base import Profile, normalize_profile_target
from .build import ArtifactMode, normalize_artifact_mode
from .output import ServeOutputConfig


def normalize_include_splits(value: object) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("include_splits must be a list of split labels")
    labels: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise ValueError("include_splits labels must be strings")
        if not item.strip():
            raise ValueError("include_splits labels must not be empty")
        if item != item.strip():
            raise ValueError("include_splits labels must not contain outer whitespace")
        if item in seen:
            raise ValueError(f"duplicate split label {item!r}")
        labels.append(item)
        seen.add(item)
    return labels


class ServeProfile(Profile):
    cmd: Literal["serve"]
    target: str
    output: ServeOutputConfig | None = None
    observability: ObservabilityConfig | None = Field(default=None)
    artifact_mode: ArtifactMode | None = Field(default=None)
    include_splits: list[str] | None = Field(default=None, min_length=1)
    limit: int | None = Field(default=None, ge=1)
    preview: PreviewStage | None = None
    throttle_ms: float | None = Field(default=None, ge=0.0)

    @field_validator("target", mode="before")
    @classmethod
    def _normalize_target(cls, value: object) -> str:
        return normalize_profile_target(value)

    @field_validator("artifact_mode", mode="before")
    @classmethod
    def _normalize_artifact_mode(cls, value: object) -> ArtifactMode | None:
        return normalize_artifact_mode(value)

    @field_validator("include_splits", mode="before")
    @classmethod
    def _normalize_include_splits(cls, value: object) -> list[str] | None:
        return normalize_include_splits(value)
