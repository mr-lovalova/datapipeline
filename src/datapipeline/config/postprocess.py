from typing import Any, List

from pydantic import RootModel, model_validator


class PostprocessConfig(RootModel[List[Any]]):
    """Schema for postprocess.yaml (list of transforms)."""

    @model_validator(mode="before")
    @classmethod
    def allow_empty(cls, value: Any) -> Any:
        if value in (None, {}):
            return []
        return value
