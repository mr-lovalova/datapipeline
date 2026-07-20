from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from datapipeline.domain.variable_id import VARIABLE_ID_SEPARATOR


NonEmptyString = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class SequenceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    size: int = Field(gt=0, strict=True)
    stride: int = Field(default=1, gt=0, strict=True)


class VariableConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    id: NonEmptyString
    stream: NonEmptyString
    field: NonEmptyString
    scale: bool = Field(default=False, strict=True)
    sequence: SequenceConfig | None = None

    @field_validator("id")
    @classmethod
    def validate_id(cls, variable_id: str) -> str:
        if VARIABLE_ID_SEPARATOR in variable_id:
            raise ValueError(
                "variable id must not contain reserved separator "
                f"{VARIABLE_ID_SEPARATOR!r}"
            )
        return variable_id
