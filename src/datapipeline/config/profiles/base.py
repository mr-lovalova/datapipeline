from pathlib import Path

from pydantic import BaseModel, Field, field_validator


class Profile(BaseModel):
    """User-facing run/build bundling and policy."""

    version: int = Field(default=1)
    type: str
    name: str
    source_path: Path | None = Field(default=None, exclude=True)
    enabled: bool = Field(default=True)

    @field_validator("name", mode="before")
    @classmethod
    def _normalize_name(cls, value):
        text = str(value).strip() if value is not None else ""
        if not text:
            raise ValueError("profile name must be set")
        return text


__all__ = ["Profile"]
