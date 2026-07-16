from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.split import (
    HASH_SPLIT_GROUP_KEY,
    HashSplitConfig,
    TimeSplitConfig,
)

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
    split_label: str | None = Field(default="train")
    folds: list[ScalerFold] | None = Field(default=None, min_length=1)
    with_mean: bool = Field(default=True, strict=True)
    with_std: bool = Field(default=True, strict=True)
    epsilon: float = Field(
        default=1e-12,
        gt=0,
        allow_inf_nan=False,
        strict=True,
    )

    @model_validator(mode="before")
    @classmethod
    def _select_folded_mode(cls, data: object) -> object:
        if (
            isinstance(data, dict)
            and data.get("folds") is not None
            and "split_label" not in data
        ):
            return {**data, "split_label": None}
        return data

    @field_validator("split_label")
    @classmethod
    def _validate_split_label(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip():
            raise ValueError("scaler split_label must not be empty")
        if value != value.strip():
            raise ValueError("scaler split_label must not contain outer whitespace")
        return value

    @model_validator(mode="after")
    def _validate_fold_config(self) -> Self:
        if self.folds is None:
            if self.split_label is None:
                raise ValueError("standard scaler operation requires split_label")
            return self
        if self.split_label is not None:
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


def validate_scaler_task_for_dataset(
    dataset: FeatureDatasetConfig,
    task: ScalerTask,
) -> None:
    if task.folds is not None:
        split = dataset.split
        if not isinstance(split, TimeSplitConfig):
            raise ValueError("Scaler folds require dataset split mode 'time'.")

        split_labels = set(split.labels)
        for fold_index, fold in enumerate(task.folds):
            unknown = (set(fold.fit) | set(fold.apply)) - split_labels
            if unknown:
                raise ValueError(
                    f"Scaler fold {fold_index} references unknown split labels: "
                    + ", ".join(sorted(unknown))
                )

        missing_apply = split_labels - {
            label for fold in task.folds for label in fold.apply
        }
        if missing_apply:
            raise ValueError(
                "Scaler folds do not apply to split labels: "
                + ", ".join(sorted(missing_apply))
            )
        return

    split_label = task.split_label
    if split_label == "all":
        return
    if split_label is None:
        raise ValueError("Standard scaler operation requires split_label.")

    split = dataset.split
    if split is None:
        raise ValueError(
            f"Cannot fit scaler split {split_label!r} without a dataset split. "
            "Use scaler split_label 'all'."
        )

    labels = set(split.labels if isinstance(split, TimeSplitConfig) else split.ratios)
    if split_label not in labels:
        raise ValueError(
            f"Scaler operation references unknown split label {split_label!r}."
        )

    if isinstance(split, HashSplitConfig) and split.key != HASH_SPLIT_GROUP_KEY:
        raise ValueError(
            "Scaler split fitting requires hash split key 'group'; "
            "feature-based split keys can change during feature processing. "
            "Use key 'group', a time split, or scaler split_label 'all'."
        )
