from pydantic import BaseModel, Field, field_validator

from .build import normalize_build_mode


class RuntimeBuildConfig(BaseModel):
    mode: str | None = Field(default=None)

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value):
        return normalize_build_mode(value)


__all__ = ["RuntimeBuildConfig"]
