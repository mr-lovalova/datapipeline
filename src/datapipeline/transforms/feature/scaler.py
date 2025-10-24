import math
from collections import defaultdict
from itertools import groupby
from numbers import Real
from pathlib import Path
from typing import Any, Iterator

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.feature.model import FeatureTransform
from datapipeline.transforms.utils import clone_record_with_value
from datapipeline.utils.pickle_model import PicklePersistanceMixin


def _iter_numeric_values(value: Any) -> Iterator[float]:
    if value is None:
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            yield from _iter_numeric_values(item)
        return
    if isinstance(value, Real):
        v = float(value)
        if not math.isnan(v):
            yield v


class StandardScaler(PicklePersistanceMixin):
    """Fit and apply per-feature scaling statistics."""

    def __init__(
        self,
        with_mean: bool = True,
        with_std: bool = True,
        epsilon: float = 1e-12,
    ) -> None:
        self.with_mean = with_mean
        self.with_std = with_std
        self.epsilon = epsilon
        self.statistics: dict[str, dict[str, float | int]] = {}

    def fit(self, vectors: Iterator[tuple[Any, Any]]) -> int:
        trackers: dict[str, StandardScaler._RunningStats] = defaultdict(
            self._RunningStats)
        total = 0
        for _, vector in vectors:
            values = getattr(vector, "values", {})
            for fid, raw in values.items():
                for value in _iter_numeric_values(raw):
                    trackers[fid].update(value)
                    total += 1

        self.statistics = {
            fid: tracker.finalize(
                with_mean=self.with_mean,
                with_std=self.with_std,
                epsilon=self.epsilon,
            )
            for fid, tracker in trackers.items()
            if tracker.count
        }
        return total

    def transform(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        if not self.statistics:
            raise RuntimeError(
                "StandardScaler must be fitted before calling transform().")

        grouped = groupby(stream, key=lambda fr: fr.id)
        for feature_id, records in grouped:
            stats = self.statistics.get(feature_id)
            if not stats:
                raise KeyError(
                    f"Missing scaler statistics for feature '{feature_id}'.")
            mean = float(stats.get("mean", 0.0))
            std = float(stats.get("std", 1.0))
            for fr in records:
                raw = self._extract_value(fr.record)
                normalized = raw
                if self.with_mean:
                    normalized -= mean
                if self.with_std:
                    normalized /= std
                yield FeatureRecord(
                    record=clone_record_with_value(fr.record, normalized),
                    id=fr.id,
                )

    @staticmethod
    def _extract_value(record: TemporalRecord) -> float:
        value = record.value
        if isinstance(value, Real):
            return float(value)
        raise TypeError(f"Record value must be numeric, got {value!r}")

    class _RunningStats:
        __slots__ = ("count", "mean", "m2")

        def __init__(self) -> None:
            self.count = 0
            self.mean = 0.0
            self.m2 = 0.0

        def update(self, value: float) -> None:
            self.count += 1
            delta = value - self.mean
            self.mean += delta / self.count
            delta2 = value - self.mean
            self.m2 += delta * delta2

        def finalize(self, *, with_mean: bool, with_std: bool, epsilon: float) -> dict[str, float | int]:
            mean = self.mean if with_mean else 0.0
            std = math.sqrt(
                self.m2 / self.count) if with_std and self.count else 1.0
            if with_std:
                std = max(std, epsilon)
            else:
                std = 1.0
            return {
                "mean": mean,
                "std": std,
                "count": self.count,
            }


class StandardScalerTransform(FeatureTransform):
    def __init__(
        self,
        *,
        model_path: str | Path | None = None,
        scaler: StandardScaler | None = None,
        with_mean: bool = True,
        with_std: bool = True,
        epsilon: float = 1e-12,
    ) -> None:
        base: StandardScaler
        if scaler is not None:
            base = scaler
        elif model_path is not None:
            base = StandardScaler.load(model_path)
        else:
            raise ValueError(
                "StandardScalerTransform requires either 'scaler' or 'model_path'.")

        if not base.statistics:
            raise RuntimeError("Loaded scaler is not fitted.")

        # Rehydrate with per-feature configuration overrides.
        self._scaler = StandardScaler(
            with_mean=with_mean,
            with_std=with_std,
            epsilon=epsilon,
        )
        self._scaler.statistics = dict(base.statistics)

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        yield from self._scaler.transform(stream)
