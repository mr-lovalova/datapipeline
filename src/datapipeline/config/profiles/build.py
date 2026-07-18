from typing import Literal

from pydantic import Field, field_validator

from datapipeline.config.observability import ObservabilityConfig

from .base import Profile, normalize_profile_operation

ArtifactMode = Literal["AUTO", "FORCE", "OFF"]
ARTIFACT_MODES: tuple[ArtifactMode, ...] = ("AUTO", "FORCE", "OFF")


def normalize_artifact_mode(value: object) -> ArtifactMode | None:
    if value is None:
        return None
    name = str(value).strip().upper()
    if name == "AUTO":
        return "AUTO"
    if name == "FORCE":
        return "FORCE"
    if name == "OFF":
        return "OFF"
    raise ValueError(f"artifact mode must be one of {', '.join(ARTIFACT_MODES)}")


class BuildProfile(Profile):
    cmd: Literal["build"]
    observability: ObservabilityConfig | None = Field(default=None)
    mode: ArtifactMode | None = Field(default=None)
    operation: str

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value: object) -> ArtifactMode | None:
        return normalize_artifact_mode(value)

    @field_validator("operation", mode="before")
    @classmethod
    def _normalize_operation(cls, value: object) -> str:
        return normalize_profile_operation(value)
