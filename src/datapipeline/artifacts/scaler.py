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


class ScalerStatistics(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    mean: float = Field(allow_inf_nan=False)
    std: float = Field(gt=0, allow_inf_nan=False)
    count: int = Field(gt=0, strict=True)


class StandardScalerArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    kind: Literal["standard_scaler"] = "standard_scaler"
    version: Literal[3] = 3
    with_mean: bool
    with_std: bool
    epsilon: float = Field(gt=0, allow_inf_nan=False)
    observations: int = Field(gt=0, strict=True)
    statistics: dict[str, ScalerStatistics] = Field(min_length=1)

    @field_validator("statistics")
    @classmethod
    def _validate_vector_ids(
        cls,
        statistics: dict[str, ScalerStatistics],
    ) -> dict[str, ScalerStatistics]:
        for vector_id in statistics:
            if not vector_id.strip():
                raise ValueError("scaler vector ids must not be empty")
            if vector_id != vector_id.strip():
                raise ValueError("scaler vector ids must not contain outer whitespace")
        return statistics

    @model_validator(mode="after")
    def _validate_observation_count(self) -> Self:
        observed = sum(statistics.count for statistics in self.statistics.values())
        if self.observations != observed:
            raise ValueError(
                "scaler observations must equal the sum of feature statistic counts"
            )
        return self


class FoldedScalerArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    kind: Literal["folded_scaler"] = "folded_scaler"
    version: Literal[3] = 3
    folds: dict[str, StandardScalerArtifact] = Field(min_length=1)

    @field_validator("folds")
    @classmethod
    def _validate_fold_ids(
        cls,
        folds: dict[str, StandardScalerArtifact],
    ) -> dict[str, StandardScalerArtifact]:
        for fold_id in folds:
            if not fold_id.strip():
                raise ValueError("scaler fold ids must not be empty")
            if fold_id != fold_id.strip():
                raise ValueError("scaler fold ids must not contain outer whitespace")
        return folds

    def for_fold(self, fold_id: str) -> StandardScalerArtifact:
        try:
            return self.folds[fold_id]
        except KeyError as exc:
            raise KeyError(f"Scaler artifact has no fold {fold_id!r}.") from exc


ScalerArtifact = Annotated[
    StandardScalerArtifact | FoldedScalerArtifact,
    Field(discriminator="kind"),
]
_SCALER_ARTIFACT: TypeAdapter[ScalerArtifact] = TypeAdapter(ScalerArtifact)


def load_scaler_artifact(path: Path) -> ScalerArtifact:
    return _SCALER_ARTIFACT.validate_json(path.read_text(encoding="utf-8"))


def save_scaler_artifact(path: Path, artifact: ScalerArtifact) -> None:
    write_json_artifact(path, artifact.model_dump(mode="json"))
