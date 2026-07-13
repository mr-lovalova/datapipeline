import math
from collections import defaultdict
from itertools import groupby
from numbers import Real
from pathlib import Path
from typing import Any, Callable, Iterator, Literal

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.pipelines.full.split import TimeLabeler
from datapipeline.transforms.feature.model import FeatureTransform
from datapipeline.dag.transform_observability import TransformEvent
from datapipeline.utils.json_artifact import read_json_artifact, write_json_artifact


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


class StandardScaler:
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
        accumulator = StandardScalerAccumulator(
            with_mean=self.with_mean,
            with_std=self.with_std,
            epsilon=self.epsilon,
        )
        total = 0
        for sample in vectors:
            vector = sample.features
            values = getattr(vector, "values", {})
            total += accumulator.observe(values)

        self.statistics = accumulator.to_scaler().statistics
        return total

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "standard_scaler",
            "version": 1,
            "with_mean": self.with_mean,
            "with_std": self.with_std,
            "epsilon": self.epsilon,
            "statistics": self.statistics,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StandardScaler":
        if payload.get("kind") != "standard_scaler":
            raise ValueError("Invalid scaler payload kind")
        scaler = cls(
            with_mean=bool(payload.get("with_mean", True)),
            with_std=bool(payload.get("with_std", True)),
            epsilon=float(payload.get("epsilon", 1e-12)),
        )
        raw_stats = payload.get("statistics")
        if not isinstance(raw_stats, dict):
            raise ValueError("Scaler payload missing 'statistics' dictionary")

        parsed_stats: dict[str, dict[str, float | int]] = {}
        for feature_id, stats in raw_stats.items():
            if not isinstance(feature_id, str) or not isinstance(stats, dict):
                continue
            parsed_stats[feature_id] = {
                "mean": float(stats.get("mean", 0.0)),
                "std": float(stats.get("std", 1.0)),
                "count": int(stats.get("count", 0)),
            }
        scaler.statistics = parsed_stats
        return scaler

    def save(
        self,
        path: str | Path,
        *,
        split: str | None = None,
        observations: int | None = None,
    ) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = self.to_dict()
        if split is not None:
            payload["split"] = split
        if observations is not None:
            payload["observations"] = int(observations)
        write_json_artifact(target, payload)

    @classmethod
    def load(cls, path: str | Path) -> "StandardScaler":
        target = Path(path)
        payload = read_json_artifact(target)
        return cls.from_dict(payload)

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
                value = fr.value
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
                    record=fr.record,
                    id=fr.id,
                    value=normalized,
                    entity_key=fr.entity_key,
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
                value = fr.value
                if not isinstance(value, Real):
                    raise TypeError(
                        f"Record value must be numeric, got {value!r}")
                restored = float(value)
                if self.with_std:
                    restored *= std
                if self.with_mean:
                    restored += mean
                yield FeatureRecord(
                    record=fr.record,
                    id=fr.id,
                    value=restored,
                    entity_key=fr.entity_key,
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


class StandardScalerAccumulator:
    def __init__(
        self,
        with_mean: bool = True,
        with_std: bool = True,
        epsilon: float = 1e-12,
    ) -> None:
        self.with_mean = with_mean
        self.with_std = with_std
        self.epsilon = epsilon
        self._trackers: dict[str, StandardScaler._RunningStats] = defaultdict(
            StandardScaler._RunningStats
        )

    def observe(self, values: dict[str, Any]) -> int:
        total = 0
        for feature_id, raw in values.items():
            for value in _iter_numeric_values(raw):
                self._trackers[feature_id].update(value)
                total += 1
        return total

    def to_scaler(self) -> StandardScaler:
        scaler = StandardScaler(
            with_mean=self.with_mean,
            with_std=self.with_std,
            epsilon=self.epsilon,
        )
        scaler.statistics = {
            feature_id: tracker.finalize(
                with_mean=self.with_mean,
                with_std=self.with_std,
                epsilon=self.epsilon,
            )
            for feature_id, tracker in self._trackers.items()
            if tracker.count
        }
        return scaler


class FeatureScaler:
    def transform(
        self,
        stream: Iterator[FeatureRecord],
        *,
        on_none: Literal["error", "skip"] = "skip",
        observer: Callable[[TransformEvent], None] | None = None,
    ) -> Iterator[FeatureRecord]:
        raise NotImplementedError

    def inverse_transform(
        self,
        stream: Iterator[FeatureRecord],
    ) -> Iterator[FeatureRecord]:
        raise NotImplementedError

    @property
    def missing_counts(self) -> dict[str, int]:
        return {}

    @classmethod
    def from_dict(
        cls,
        payload: dict[str, Any],
        *,
        with_mean: bool = True,
        with_std: bool = True,
        epsilon: float = 1e-12,
    ) -> "FeatureScaler":
        kind = payload.get("kind")
        if kind == "standard_scaler":
            return StandardFeatureScaler(
                _scaler_with_options(
                    StandardScaler.from_dict(payload),
                    with_mean=with_mean,
                    with_std=with_std,
                    epsilon=epsilon,
                )
            )
        if kind == "temporal_scaler":
            return TemporalFeatureScaler.from_dict(
                payload,
                with_mean=with_mean,
                with_std=with_std,
                epsilon=epsilon,
            )
        raise ValueError("Invalid scaler payload kind")

    @classmethod
    def load(
        cls,
        path: str | Path,
        *,
        with_mean: bool = True,
        with_std: bool = True,
        epsilon: float = 1e-12,
    ) -> "FeatureScaler":
        return cls.from_dict(
            read_json_artifact(Path(path)),
            with_mean=with_mean,
            with_std=with_std,
            epsilon=epsilon,
        )


def _scaler_with_options(
    scaler: StandardScaler,
    *,
    with_mean: bool,
    with_std: bool,
    epsilon: float,
) -> StandardScaler:
    if not scaler.statistics:
        raise RuntimeError("Loaded scaler is not fitted.")
    configured = StandardScaler(
        with_mean=with_mean,
        with_std=with_std,
        epsilon=epsilon,
    )
    configured.statistics = dict(scaler.statistics)
    return configured


class StandardFeatureScaler(FeatureScaler):
    def __init__(self, scaler: StandardScaler) -> None:
        self._scaler = scaler

    @property
    def missing_counts(self) -> dict[str, int]:
        return dict(self._scaler.missing_counts)

    def transform(
        self,
        stream: Iterator[FeatureRecord],
        *,
        on_none: Literal["error", "skip"] = "skip",
        observer: Callable[[TransformEvent], None] | None = None,
    ) -> Iterator[FeatureRecord]:
        yield from self._scaler.transform(
            stream,
            on_none=on_none,
            observer=observer,
        )

    def inverse_transform(
        self,
        stream: Iterator[FeatureRecord],
    ) -> Iterator[FeatureRecord]:
        yield from self._scaler.inverse_transform(stream)


class TemporalFeatureScaler(FeatureScaler):
    def __init__(
        self,
        *,
        labeler: TimeLabeler,
        scalers_by_label: dict[str, StandardScaler],
    ) -> None:
        self._labeler = labeler
        self._scalers_by_label = dict(scalers_by_label)
        self._missing_counts: dict[str, int] = {}

    @classmethod
    def from_dict(
        cls,
        payload: dict[str, Any],
        *,
        with_mean: bool = True,
        with_std: bool = True,
        epsilon: float = 1e-12,
    ) -> "TemporalFeatureScaler":
        if payload.get("kind") != "temporal_scaler":
            raise ValueError("Invalid temporal scaler payload kind")
        split = payload.get("split")
        if not isinstance(split, dict) or split.get("mode") != "time":
            raise ValueError("Temporal scaler payload missing time split config")
        boundaries = split.get("boundaries")
        labels = split.get("labels")
        if not isinstance(boundaries, list) or not isinstance(labels, list):
            raise ValueError("Temporal scaler split requires boundaries and labels")

        scalers_by_label: dict[str, StandardScaler] = {}
        folds = payload.get("folds")
        if not isinstance(folds, list) or not folds:
            raise ValueError("Temporal scaler payload requires folds")
        for index, fold in enumerate(folds):
            if not isinstance(fold, dict):
                raise ValueError(f"Temporal scaler fold {index} must be a mapping")
            apply_labels = fold.get("apply")
            if not isinstance(apply_labels, list) or not apply_labels:
                raise ValueError(f"Temporal scaler fold {index} requires apply labels")
            scaler_payload = fold.get("scaler")
            if not isinstance(scaler_payload, dict):
                raise ValueError(f"Temporal scaler fold {index} requires scaler payload")
            scaler = _scaler_with_options(
                StandardScaler.from_dict(scaler_payload),
                with_mean=with_mean,
                with_std=with_std,
                epsilon=epsilon,
            )
            for label in apply_labels:
                label_text = str(label)
                if label_text in scalers_by_label:
                    raise ValueError(
                        f"Temporal scaler apply label {label_text!r} is duplicated"
                    )
                scalers_by_label[label_text] = scaler

        return cls(
            labeler=TimeLabeler(boundaries=boundaries, labels=labels),
            scalers_by_label=scalers_by_label,
        )

    @property
    def missing_counts(self) -> dict[str, int]:
        return dict(self._missing_counts)

    def transform(
        self,
        stream: Iterator[FeatureRecord],
        *,
        on_none: Literal["error", "skip"] = "skip",
        observer: Callable[[TransformEvent], None] | None = None,
    ) -> Iterator[FeatureRecord]:
        if not self._scalers_by_label:
            raise RuntimeError("TemporalFeatureScaler must include at least one scaler.")

        self._missing_counts = {}
        for fr in stream:
            label = self._label_for_feature_record(fr)
            scaler = self._scalers_by_label.get(label)
            if scaler is None:
                raise KeyError(f"No scaler fold applies to split label {label!r}.")
            yield self._transform_record(
                scaler=scaler,
                fr=fr,
                on_none=on_none,
                observer=observer,
            )

    def inverse_transform(
        self,
        stream: Iterator[FeatureRecord],
    ) -> Iterator[FeatureRecord]:
        raise NotImplementedError("Temporal scaler inverse transform is not supported.")

    def _label_for_feature_record(self, fr: FeatureRecord) -> str:
        record_time = getattr(fr.record, "time", None)
        if record_time is None:
            raise ValueError("Temporal scaler requires feature records with record.time")
        return self._labeler.label(record_time, Vector(values={}))

    def _transform_record(
        self,
        *,
        scaler: StandardScaler,
        fr: FeatureRecord,
        on_none: Literal["error", "skip"],
        observer: Callable[[TransformEvent], None] | None,
    ) -> FeatureRecord:
        stats = scaler.statistics.get(fr.id)
        if not stats:
            raise KeyError(f"Missing scaler statistics for feature '{fr.id}'.")

        value = fr.value
        if not isinstance(value, Real):
            if value is None and on_none == "skip":
                self._missing_counts[fr.id] = self._missing_counts.get(fr.id, 0) + 1
                if observer is not None:
                    observer(
                        TransformEvent(
                            type="scaler_none",
                            payload={
                                "feature_id": fr.id,
                                "record": fr.record,
                                "count": self._missing_counts[fr.id],
                            },
                        )
                    )
                return fr
            raise TypeError(f"Record value must be numeric, got {value!r}")

        normalized = float(value)
        if scaler.with_mean:
            normalized -= float(stats.get("mean", 0.0))
        if scaler.with_std:
            normalized /= float(stats.get("std", 1.0))
        return FeatureRecord(
            record=fr.record,
            id=fr.id,
            value=normalized,
            entity_key=fr.entity_key,
        )


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
        base: FeatureScaler
        if scaler is not None:
            base = StandardFeatureScaler(
                _scaler_with_options(
                    scaler,
                    with_mean=with_mean,
                    with_std=with_std,
                    epsilon=epsilon,
                )
            )
        elif model_path is not None:
            base = FeatureScaler.load(
                model_path,
                with_mean=with_mean,
                with_std=with_std,
                epsilon=epsilon,
            )
        else:
            raise ValueError(
                "StandardScalerTransform requires either 'scaler' or 'model_path'.")

        self._scaler = base
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
