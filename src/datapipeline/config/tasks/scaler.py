from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .base import ArtifactTask


class ScalerFold(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    fit: list[str] = Field(min_length=1)
    apply: list[str] = Field(min_length=1)

    @field_validator("fit", "apply")
    @classmethod
    def _validate_labels(cls, value: list[str]) -> list[str]:
        for label in value:
            if not label.strip():
                raise ValueError("scaler fold labels must not be empty")
            if label != label.strip():
                raise ValueError("scaler fold labels must not contain outer whitespace")
        if len(value) != len(set(value)):
            raise ValueError("scaler fold labels must be unique")
        return value


class ScalerTask(ArtifactTask):
    id: Literal["scaler"] = Field(default="scaler")
    entrypoint: str = Field(default="core.artifact.scaler")
    output: str = Field(default="build/scaler.json")
    split_label: str = Field(default="train")
    folds: list[ScalerFold] | None = Field(default=None, min_length=1)
    with_mean: bool = Field(default=True, strict=True)
    with_std: bool = Field(default=True, strict=True)
    epsilon: float = Field(
        default=1e-12,
        gt=0,
        allow_inf_nan=False,
        strict=True,
    )

    @field_validator("split_label")
    @classmethod
    def _validate_split_label(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("scaler split_label must not be empty")
        if value != value.strip():
            raise ValueError("scaler split_label must not contain outer whitespace")
        return value

    @model_validator(mode="after")
    def _validate_fold_config(self) -> Self:
        if self.folds is None:
            return self
        if "split_label" in self.model_fields_set:
            raise ValueError(
                "scaler operation cannot define both split_label and folds"
            )

        applied: dict[str, int] = {}
        for index, fold in enumerate(self.folds):
            for label in fold.apply:
                existing = applied.get(label)
                if existing is not None:
                    raise ValueError(
                        f"split label {label!r} is applied by scaler folds "
                        f"{existing} and {index}"
                    )
                applied[label] = index
        return self
