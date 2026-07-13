from typing import Annotated, Any

from pydantic import BeforeValidator, PlainSerializer, RootModel, model_validator

from datapipeline.transforms.spec import (
    TransformSpec,
    parse_transform_spec,
    serialize_transform_spec,
)


_ConfiguredTransform = Annotated[
    TransformSpec,
    BeforeValidator(parse_transform_spec),
    PlainSerializer(
        serialize_transform_spec,
        return_type=dict[str, dict[str, Any]],
    ),
]


class PostprocessConfig(RootModel[list[_ConfiguredTransform]]):
    """Schema for postprocess.yaml (list of transforms)."""

    @model_validator(mode="before")
    @classmethod
    def allow_empty(cls, value: Any) -> Any:
        if value in (None, {}):
            return []
        return value
