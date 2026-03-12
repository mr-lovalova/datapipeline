from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator
from datapipeline.config.model_utils import normalize_required_text


class Profile(BaseModel):
    """User-facing run/build bundling and policy."""

    model_config = ConfigDict(extra="forbid")

    cmd: str
    name: str
    order: int | None = Field(default=None, ge=0)
    source_path: Path | None = Field(default=None, exclude=True)
    enabled: bool = Field(default=True)

    @field_validator("name", mode="before")
    @classmethod
    def _normalize_name(cls, value):
        text = str(value).strip() if value is not None else ""
        if not text:
            raise ValueError("profile name must be set")
        return text

def normalize_profile_target(value) -> str:
    return normalize_required_text(
        value,
        field_name="target",
        lower=True,
    )


__all__ = ["Profile", "normalize_profile_target"]
