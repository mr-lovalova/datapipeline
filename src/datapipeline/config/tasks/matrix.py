from typing import Literal

from pydantic import Field, field_validator

from .base import OperationTask


class MatrixTask(OperationTask[dict[str, object]]):
    entrypoint: Literal["core.runtime.matrix"] = Field(default="core.runtime.matrix")
    options: dict[str, object] = Field(default_factory=dict)

    @field_validator("options")
    @classmethod
    def _reject_options(cls, value: dict[str, object]) -> dict[str, object]:
        if value:
            raise ValueError("matrix task does not accept options")
        return value


__all__ = ["MatrixTask"]
