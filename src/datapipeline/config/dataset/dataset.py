from typing import Annotated, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from datapipeline.config.dataset.variable import VariableConfig
from datapipeline.config.dataset.postprocess import PostprocessConfig
from datapipeline.config.dataset.split import HashSplitConfig, SplitConfig
from datapipeline.utils.time import CADENCE_PATTERN


NonEmptyString = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1),
]


class SampleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    cadence: str = Field(..., pattern=CADENCE_PATTERN)
    keys: list[NonEmptyString] = Field(default_factory=list)

    @field_validator("keys")
    @classmethod
    def validate_keys(cls, keys: list[str]) -> list[str]:
        if any(not key.strip() for key in keys):
            raise ValueError("sample keys must not be empty")
        if len(keys) != len(set(keys)):
            raise ValueError("sample keys must not contain duplicates")
        return keys


class DatasetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    sample: SampleConfig
    features: list[VariableConfig] = Field(default_factory=list)
    targets: list[VariableConfig] = Field(default_factory=list)
    split: SplitConfig | None = None
    postprocess: PostprocessConfig = Field(default_factory=PostprocessConfig)

    @property
    def variables(self) -> tuple[VariableConfig, ...]:
        return (*self.features, *self.targets)

    @model_validator(mode="after")
    def validate_variables(self) -> Self:
        if self.targets and not self.features:
            raise ValueError("datasets with targets must define at least one feature")
        seen: set[str] = set()
        for config in self.variables:
            if config.id in seen:
                raise ValueError(
                    f"dataset variable id {config.id!r} must be unique across "
                    "features and targets"
                )
            seen.add(config.id)
        return self

    @model_validator(mode="after")
    def validate_hash_split_sequences(self) -> Self:
        if not isinstance(self.split, HashSplitConfig):
            return self
        sequenced = [
            config.id for config in self.variables if config.sequence is not None
        ]
        if sequenced:
            raise ValueError(
                "hash splits cannot be used with sequenced features or targets: "
                + ", ".join(sequenced)
            )
        return self
