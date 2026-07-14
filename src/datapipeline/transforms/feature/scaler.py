import math
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from numbers import Real
from typing import Iterator

from datapipeline.artifacts.scaler import (
    ScalerArtifact,
    ScalerStatistics,
    StandardScalerArtifact,
    TemporalScalerArtifact,
)
from datapipeline.domain.feature import FeatureRecord


@dataclass(slots=True)
class _RunningStatistics:
    count: int = 0
    mean: float = 0.0
    squared_deviations: float = 0.0

    def observe(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        self.squared_deviations += delta * (value - self.mean)

    def finish(self, epsilon: float) -> ScalerStatistics:
        variance = self.squared_deviations / self.count
        return ScalerStatistics(
            mean=self.mean,
            std=max(math.sqrt(max(variance, 0.0)), epsilon),
            count=self.count,
        )


class ScalerAccumulator:
    def __init__(
        self,
        with_mean: bool = True,
        with_std: bool = True,
        epsilon: float = 1e-12,
    ) -> None:
        if not isinstance(with_mean, bool):
            raise TypeError("with_mean must be a boolean")
        if not isinstance(with_std, bool):
            raise TypeError("with_std must be a boolean")
        if isinstance(epsilon, bool) or not isinstance(epsilon, Real):
            raise TypeError("epsilon must be numeric")
        epsilon = float(epsilon)
        if not math.isfinite(epsilon) or epsilon <= 0:
            raise ValueError("epsilon must be finite and positive")

        self.with_mean: bool = with_mean
        self.with_std: bool = with_std
        self.epsilon: float = epsilon
        self._statistics: dict[str, _RunningStatistics] = defaultdict(
            _RunningStatistics
        )
        self.observations: int = 0

    def observe(self, feature_id: str, value: object) -> None:
        if not feature_id.strip():
            raise ValueError("feature_id must not be empty")
        if value is None:
            return
        numeric = _finite_number(value)
        self._statistics[feature_id].observe(numeric)
        self.observations += 1

    def artifact(self, split: str | None = None) -> StandardScalerArtifact:
        if not self._statistics:
            raise RuntimeError("Scaler fitting produced no numeric observations.")
        return StandardScalerArtifact(
            with_mean=self.with_mean,
            with_std=self.with_std,
            epsilon=self.epsilon,
            observations=self.observations,
            statistics={
                feature_id: statistics.finish(self.epsilon)
                for feature_id, statistics in self._statistics.items()
            },
            split=split,
        )


class FeatureScaler:
    def __init__(self, artifact: ScalerArtifact) -> None:
        self.artifact = artifact
        self._boundaries: tuple[datetime, ...] = ()
        self._labels: tuple[str, ...] = ()
        self._scalers_by_label: dict[str, StandardScalerArtifact] = {}
        if isinstance(artifact, TemporalScalerArtifact):
            self._boundaries = tuple(
                datetime.fromisoformat(boundary.replace("Z", "+00:00"))
                for boundary in artifact.split.boundaries
            )
            self._labels = artifact.split.labels
            self._scalers_by_label = {
                label: fold.scaler for fold in artifact.folds for label in fold.apply
            }

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        for feature in stream:
            yield self.scale(feature)

    def scale(self, feature: FeatureRecord) -> FeatureRecord:
        return _scale_feature(feature, self._scaler_for(feature))

    def _scaler_for(self, feature: FeatureRecord) -> StandardScalerArtifact:
        if isinstance(self.artifact, StandardScalerArtifact):
            return self.artifact

        label = self._labels[bisect_right(self._boundaries, feature.record.time)]
        return self._scalers_by_label[label]


def _scale_feature(
    feature: FeatureRecord,
    scaler: StandardScalerArtifact,
) -> FeatureRecord:
    if feature.value is None:
        return feature
    try:
        statistics = scaler.statistics[feature.id]
    except KeyError as exc:
        raise KeyError(
            f"Missing scaler statistics for feature {feature.id!r}."
        ) from exc

    value = _finite_number(feature.value)
    if scaler.with_mean:
        value -= statistics.mean
    if scaler.with_std:
        value /= statistics.std
    return FeatureRecord(
        record=feature.record,
        id=feature.id,
        value=value,
        entity_key=feature.entity_key,
    )


def _finite_number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"Feature value must be numeric or None, got {value!r}.")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"Feature value must be finite, got {value!r}.")
    return number
