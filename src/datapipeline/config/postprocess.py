from typing import Any, List
from pydantic import RootModel, model_validator


class PostprocessConfig(RootModel[List[Any]]):
    """Schema for optional postprocess.yaml."""

    @model_validator(mode="before")
    @classmethod
    def allow_empty(cls, value: Any) -> Any:
        """Coerce missing or empty mappings into an empty list."""
        if value in (None, {}):
            return []
        return value
