from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .base import ArtifactTask


class ScalerFold(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fit: list[str] = Field(min_length=1)
    apply: list[str] = Field(min_length=1)

    @field_validator("fit", "apply", mode="before")
    @classmethod
    def _normalize_labels(cls, value):
        if isinstance(value, str):
            return [value]
        return value

    @field_validator("fit", "apply")
    @classmethod
    def _validate_labels(cls, value: list[str]) -> list[str]:
        labels: list[str] = []
        seen: set[str] = set()
        for item in value:
            label = str(item).strip()
            if not label:
                raise ValueError("scaler fold labels must not be empty")
            if label in seen:
                raise ValueError(f"duplicate scaler fold label {label!r}")
            labels.append(label)
            seen.add(label)
        return labels


class ScalerTask(ArtifactTask):
    id: Literal["scaler"] = Field(default="scaler")
    entrypoint: str = Field(default="core.artifact.scaler")
    output: str = Field(default="build/scaler.json")
    split_label: str = Field(default="train")
    folds: list[ScalerFold] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _validate_fold_config(self):
        if self.folds is None:
            return self
        fields_set = getattr(self, "model_fields_set", set())
        if "split_label" in fields_set:
            raise ValueError("scaler task cannot define both split_label and folds")

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


__all__ = ["ScalerFold", "ScalerTask"]
