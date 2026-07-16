import math
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from datapipeline.utils.time import parse_datetime


HASH_SPLIT_GROUP_KEY = "group"
HASH_SPLIT_FEATURE_PREFIX = "feature:"

Ratio = Annotated[float, Field(gt=0.0, le=1.0)]


def _validate_output_labels(labels: list[str] | None) -> list[str] | None:
    if labels is None:
        return None
    for label in labels:
        if not label.strip():
            raise ValueError("split output labels must not be empty")
        if label != label.strip():
            raise ValueError("split output labels must not contain outer whitespace")
    if len(labels) != len(set(labels)):
        raise ValueError("split output labels must be unique")
    return labels


class HashSplitConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    mode: Literal["hash"] = "hash"
    ratios: dict[str, Ratio]
    output_labels: list[str] | None = Field(default=None, min_length=1)
    seed: int = 42
    key: str = HASH_SPLIT_GROUP_KEY

    @field_validator("key")
    @classmethod
    def validate_key(cls, key: str) -> str:
        if key == HASH_SPLIT_GROUP_KEY:
            return key
        if key.startswith(HASH_SPLIT_FEATURE_PREFIX):
            if key.removeprefix(HASH_SPLIT_FEATURE_PREFIX):
                return key
            raise ValueError("hash split key must include a feature id")
        raise ValueError("hash split key must be 'group' or 'feature:<id>'")

    @field_validator("ratios")
    @classmethod
    def validate_ratios(cls, ratios: dict[str, float]) -> dict[str, float]:
        if not ratios:
            raise ValueError("hash split ratios must not be empty")
        for label in ratios:
            if not label.strip():
                raise ValueError("hash split labels must not be empty")
            if label != label.strip():
                raise ValueError("hash split labels must not contain outer whitespace")
        total = sum(ratios.values())
        if not math.isclose(total, 1.0, rel_tol=1e-9, abs_tol=1e-9):
            raise ValueError(f"hash split ratios must sum to 1.0 (got {total})")
        return dict(sorted(ratios.items()))

    @field_validator("output_labels")
    @classmethod
    def validate_output_labels(cls, labels: list[str] | None) -> list[str] | None:
        return _validate_output_labels(labels)

    @model_validator(mode="after")
    def validate_output_labels_exist(self) -> Self:
        if self.output_labels is None:
            return self
        unknown = set(self.output_labels) - set(self.ratios)
        if unknown:
            raise ValueError(
                "split output labels are not defined by ratios: "
                + ", ".join(sorted(unknown))
            )
        return self


class TimeSplitConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    mode: Literal["time"] = "time"
    boundaries: list[str]
    labels: list[str] = Field(min_length=1)
    output_labels: list[str] | None = Field(default=None, min_length=1)

    @field_validator("boundaries")
    @classmethod
    def validate_boundaries(cls, boundaries: list[str]) -> list[str]:
        parsed = []
        for boundary in boundaries:
            if not boundary.strip():
                raise ValueError("time split boundaries must not be empty")
            if boundary != boundary.strip():
                raise ValueError(
                    "time split boundaries must not contain outer whitespace"
                )
            parsed.append(parse_datetime(boundary))
        if any(previous >= current for previous, current in zip(parsed, parsed[1:])):
            raise ValueError("time split boundaries must be strictly increasing")
        return boundaries

    @field_validator("labels")
    @classmethod
    def validate_labels(cls, labels: list[str]) -> list[str]:
        for label in labels:
            if not label.strip():
                raise ValueError("time split labels must not be empty")
            if label != label.strip():
                raise ValueError("time split labels must not contain outer whitespace")
        if len(labels) != len(set(labels)):
            raise ValueError("time split labels must be unique")
        return labels

    @field_validator("output_labels")
    @classmethod
    def validate_output_labels(cls, labels: list[str] | None) -> list[str] | None:
        return _validate_output_labels(labels)

    @model_validator(mode="after")
    def validate_label_count(self) -> Self:
        if len(self.labels) != len(self.boundaries) + 1:
            raise ValueError("time split labels length must equal len(boundaries) + 1")
        if self.output_labels is not None:
            unknown = set(self.output_labels) - set(self.labels)
            if unknown:
                raise ValueError(
                    "split output labels are not defined by labels: "
                    + ", ".join(sorted(unknown))
                )
        return self


SplitConfig = Annotated[
    HashSplitConfig | TimeSplitConfig,
    Field(discriminator="mode"),
]


def split_output_labels(config: SplitConfig) -> tuple[str, ...]:
    if config.output_labels is not None:
        return tuple(config.output_labels)
    if isinstance(config, TimeSplitConfig):
        return tuple(config.labels)
    return tuple(config.ratios)
