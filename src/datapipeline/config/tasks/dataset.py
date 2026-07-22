from typing import Literal

from pydantic import Field, field_validator

from .base import RuntimeTask


class DatasetTask(RuntimeTask[dict[str, object]]):
    entrypoint: Literal["core.runtime.dataset"] = Field(default="core.runtime.dataset")
    options: dict[str, object] = Field(default_factory=dict)

    @field_validator("options")
    @classmethod
    def _reject_options(cls, value: dict[str, object]) -> dict[str, object]:
        if value:
            raise ValueError("dataset operation does not accept options")
        return value
