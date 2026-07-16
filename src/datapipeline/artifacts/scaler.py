from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)

from datapipeline.utils.json_artifact import write_json_artifact
from datapipeline.utils.time import parse_datetime


class ScalerStatistics(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    mean: float = Field(allow_inf_nan=False)
    std: float = Field(gt=0, allow_inf_nan=False)
    count: int = Field(gt=0, strict=True)


class StandardScalerArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    kind: Literal["standard_scaler"] = "standard_scaler"
    version: Literal[2] = 2
    with_mean: bool
    with_std: bool
    epsilon: float = Field(gt=0, allow_inf_nan=False)
    observations: int = Field(gt=0, strict=True)
    statistics: dict[str, ScalerStatistics] = Field(min_length=1)
    split: str | None = None

    @field_validator("statistics")
    @classmethod
    def _validate_feature_ids(
        cls,
        statistics: dict[str, ScalerStatistics],
    ) -> dict[str, ScalerStatistics]:
        for feature_id in statistics:
            if not feature_id.strip():
                raise ValueError("scaler feature ids must not be empty")
            if feature_id != feature_id.strip():
                raise ValueError("scaler feature ids must not contain outer whitespace")
        return statistics

    @field_validator("split")
    @classmethod
    def _validate_split(cls, split: str | None) -> str | None:
        if split is not None and not split.strip():
            raise ValueError("scaler split must not be empty")
        if split is not None and split != split.strip():
            raise ValueError("scaler split must not contain outer whitespace")
        return split

    @model_validator(mode="after")
    def _validate_observation_count(self) -> Self:
        observed = sum(statistics.count for statistics in self.statistics.values())
        if self.observations != observed:
            raise ValueError(
                "scaler observations must equal the sum of feature statistic counts"
            )
        return self


class TemporalScalerSplit(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    mode: Literal["time"] = "time"
    boundaries: tuple[str, ...]
    labels: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_time_split(self) -> Self:
        if len(self.labels) != len(self.boundaries) + 1:
            raise ValueError(
                "temporal scaler labels must contain one more item than boundaries"
            )
        if any(not label.strip() for label in self.labels):
            raise ValueError("temporal scaler labels must not be empty")
        if any(label != label.strip() for label in self.labels):
            raise ValueError("temporal scaler labels must not contain outer whitespace")
        if len(set(self.labels)) != len(self.labels):
            raise ValueError("temporal scaler labels must be unique")

        parsed_boundaries: list[datetime] = []
        for boundary in self.boundaries:
            if not boundary.strip():
                raise ValueError("temporal scaler boundaries must not be empty")
            try:
                parsed = parse_datetime(boundary)
            except ValueError as exc:
                raise ValueError(
                    f"invalid temporal scaler boundary {boundary!r}"
                ) from exc
            parsed_boundaries.append(parsed)
        if any(
            previous >= current
            for previous, current in zip(parsed_boundaries, parsed_boundaries[1:])
        ):
            raise ValueError("temporal scaler boundaries must be strictly increasing")
        return self


class TemporalScalerFold(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    fit: tuple[str, ...] = Field(min_length=1)
    apply: tuple[str, ...] = Field(min_length=1)
    scaler: StandardScalerArtifact

    @field_validator("fit", "apply")
    @classmethod
    def _validate_labels(cls, labels: tuple[str, ...]) -> tuple[str, ...]:
        if any(not label.strip() for label in labels):
            raise ValueError("scaler fold labels must not be empty")
        if any(label != label.strip() for label in labels):
            raise ValueError("scaler fold labels must not contain outer whitespace")
        if len(set(labels)) != len(labels):
            raise ValueError("scaler fold labels must be unique")
        return labels

    @model_validator(mode="after")
    def _reject_nested_split(self) -> Self:
        if self.scaler.split is not None:
            raise ValueError("temporal fold scaler must not define split")
        return self


class TemporalScalerArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    kind: Literal["temporal_scaler"] = "temporal_scaler"
    version: Literal[2] = 2
    split: TemporalScalerSplit
    folds: tuple[TemporalScalerFold, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_folds(self) -> Self:
        split_labels = set(self.split.labels)
        applied_labels: set[str] = set()
        options: set[tuple[bool, bool, float]] = set()
        for fold in self.folds:
            unknown = (set(fold.fit) | set(fold.apply)) - split_labels
            if unknown:
                raise ValueError(
                    "temporal scaler fold references unknown labels: "
                    + ", ".join(sorted(unknown))
                )
            duplicate_apply = applied_labels & set(fold.apply)
            if duplicate_apply:
                raise ValueError(
                    "temporal scaler labels have multiple apply folds: "
                    + ", ".join(sorted(duplicate_apply))
                )
            applied_labels.update(fold.apply)
            options.add(
                (fold.scaler.with_mean, fold.scaler.with_std, fold.scaler.epsilon)
            )
        missing_apply = split_labels - applied_labels
        if missing_apply:
            raise ValueError(
                "temporal scaler labels have no apply fold: "
                + ", ".join(sorted(missing_apply))
            )
        if len(options) != 1:
            raise ValueError("all temporal scaler folds must use the same options")
        return self


ScalerArtifact = Annotated[
    StandardScalerArtifact | TemporalScalerArtifact,
    Field(discriminator="kind"),
]
_SCALER_ARTIFACT: TypeAdapter[ScalerArtifact] = TypeAdapter(ScalerArtifact)


def load_scaler_artifact(path: Path) -> ScalerArtifact:
    return _SCALER_ARTIFACT.validate_json(path.read_text(encoding="utf-8"))


def save_scaler_artifact(path: Path, artifact: ScalerArtifact) -> None:
    write_json_artifact(path, artifact.model_dump(mode="json"))
