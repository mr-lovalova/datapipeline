from typing import Literal

from pydantic import Field, field_validator

from .base import OperationTask


class PipelineTask(OperationTask[dict[str, object]]):
    entrypoint: Literal["core.runtime.pipeline"] = Field(
        default="core.runtime.pipeline"
    )
    options: dict[str, object] = Field(default_factory=dict)

    @field_validator("options")
    @classmethod
    def _reject_options(cls, value: dict[str, object]) -> dict[str, object]:
        if value:
            raise ValueError("pipeline operation does not accept options")
        return value
