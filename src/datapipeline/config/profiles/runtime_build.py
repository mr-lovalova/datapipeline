from pydantic import BaseModel, Field, field_validator

from .build import VALID_BUILD_MODES


class RuntimeBuildConfig(BaseModel):
    mode: str | None = Field(default=None)

    @field_validator("mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value):
        if value is None:
            return None
        if isinstance(value, bool):
            return "OFF" if value is False else "AUTO"
        name = str(value).strip().upper()
        if name not in VALID_BUILD_MODES:
            raise ValueError(
                f"mode must be one of {', '.join(VALID_BUILD_MODES)}, got {value!r}"
            )
        return name


__all__ = ["RuntimeBuildConfig"]
