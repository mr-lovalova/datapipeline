import math
from collections import defaultdict
from itertools import groupby
from numbers import Real
from pathlib import Path
from typing import Any, Callable, Iterator, Literal, Mapping

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.sample import Sample
from datapipeline.transforms.feature.model import FeatureTransform
from datapipeline.transforms.utils import clone_record_with_value
from datapipeline.utils.pickle_model import PicklePersistanceMixin
from datapipeline.pipeline.observability import TransformEvent


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
        self.missing_counts: dict[str, int] = {}

    def fit(self, vectors: Iterator[Sample]) -> int:
        trackers: dict[str, StandardScaler._RunningStats] = defaultdict(
            self._RunningStats)
        total = 0
        for sample in vectors:
            vector = sample.features
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

    def transform(
        self,
        stream: Iterator[FeatureRecord],
        *,
        on_none: Literal["error", "skip"] = "skip",
        observer: Callable[[TransformEvent], None] | None = None,
    ) -> Iterator[FeatureRecord]:
        if not self.statistics:
            raise RuntimeError(
                "StandardScaler must be fitted before calling transform().")

        self.missing_counts = {}

        grouped = groupby(stream, key=lambda fr: fr.id)
        for feature_id, records in grouped:
            stats = self.statistics.get(feature_id)
            if not stats:
                raise KeyError(
                    f"Missing scaler statistics for feature '{feature_id}'.")
            mean = float(stats.get("mean", 0.0))
            std = float(stats.get("std", 1.0))
            for fr in records:
                value = fr.record.value
                if not isinstance(value, Real):
                    if value is None and on_none == "skip":
                        self.missing_counts[feature_id] = (
                            self.missing_counts.get(feature_id, 0) + 1
                        )
                        if observer is not None:
                            observer(
                                TransformEvent(
                                    type="scaler_none",
                                    payload={
                                        "feature_id": feature_id,
                                        "record": fr.record,
                                        "count": self.missing_counts[feature_id],
                                    },
                                )
                            )
                        yield fr
                        continue
                    raise TypeError(
                        f"Record value must be numeric, got {value!r}")

                raw = float(value)
                normalized = raw
                if self.with_mean:
                    normalized -= mean
                if self.with_std:
                    normalized /= std
                yield FeatureRecord(
                    record=clone_record_with_value(fr.record, normalized),
                    id=fr.id,
                )

    def inverse_transform(
        self,
        stream: Iterator[FeatureRecord],
    ) -> Iterator[FeatureRecord]:
        if not self.statistics:
            raise RuntimeError(
                "StandardScaler must be fitted before calling inverse_transform().")

        grouped = groupby(stream, key=lambda fr: fr.id)
        for feature_id, records in grouped:
            stats = self.statistics.get(feature_id)
            if not stats:
                raise KeyError(
                    f"Missing scaler statistics for feature '{feature_id}'.")
            mean = float(stats.get("mean", 0.0))
            std = float(stats.get("std", 1.0))
            for fr in records:
                value = fr.record.value
                if not isinstance(value, Real):
                    raise TypeError(
                        f"Record value must be numeric, got {value!r}")
                restored = float(value)
                if self.with_std:
                    restored *= std
                if self.with_mean:
                    restored += mean
                yield FeatureRecord(
                    record=clone_record_with_value(fr.record, restored),
                    id=fr.id,
                )

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
        on_none: Literal["error", "skip"] = "skip",
        observer: Callable[[TransformEvent], None] | None = None,
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
        self._on_none = on_none
        self._observer = observer

    @property
    def missing_counts(self) -> dict[str, int]:
        return dict(self._scaler.missing_counts)

    def set_observer(self, observer: Callable[[TransformEvent], None] | None) -> None:
        self._observer = observer

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        yield from self._scaler.transform(
            stream,
            on_none=self._on_none,
            observer=self._observer,
        )

    def inverse(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        """Undo scaling using the fitted statistics."""
        yield from self._scaler.inverse_transform(stream)
