from dataclasses import is_dataclass, replace
from itertools import groupby
from math import sqrt
from numbers import Real
from typing import Any, Iterator, Mapping, MutableMapping

from datapipeline.domain.feature import FeatureRecord
from datapipeline.transforms.feature.model import FeatureTransform
from datapipeline.domain.record import TemporalRecord


def _clone_with_value(record: Any, value: float) -> Any:
    if isinstance(record, Mapping):
        cloned: MutableMapping[str, Any] = type(record)(record)
        cloned["value"] = value
        return cloned

    if hasattr(record, "value"):
        if is_dataclass(record):
            return replace(record, value=value)
        cloned = type(record)(**record.__dict__)
        cloned.value = value
        return cloned

    raise TypeError(f"Cannot replace value on record type: {type(record)!r}")


class StandardScalerTransform(FeatureTransform):
    """Standardize feature values to zero mean and unit variance per feature id."""

    def __init__(
        self,
        *,
        with_mean: bool = True,
        with_std: bool = True,
        epsilon: float = 1e-12,
        statistics: Mapping[str, Mapping[str, float]] | None = None,
    ) -> None:
        self.with_mean = with_mean
        self.with_std = with_std
        self.epsilon = epsilon
        self.statistics = dict(statistics or {})
        self.stats_: dict[str, dict[str, float]] = {}

    def _resolve_stats(
        self, feature_id: str, values: list[float]
    ) -> tuple[float, float]:
        if feature_id in self.statistics:
            stats = self.statistics[feature_id]
            mean = float(stats.get("mean", 0.0))
            std = float(stats.get("std", 1.0))
        else:
            mean = sum(values) / len(values) if self.with_mean else 0.0
            if self.with_std:
                variance = sum((v - mean) ** 2 for v in values) / len(values)
                std = sqrt(variance)
            else:
                std = 1.0
            self.stats_[feature_id] = {
                "mean": mean if self.with_mean else 0.0,
                "std": std if self.with_std else 1.0,
            }
        if self.with_std:
            std = max(std, self.epsilon)
        else:
            std = 1.0
        return (mean if self.with_mean else 0.0, std)

    def _extract_value(self, record: TemporalRecord) -> float:
        value = record.value
        if isinstance(value, Real):
            return float(value)
        raise TypeError(f"Record value must be numeric, got {value!r}")

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        grouped = groupby(stream, key=lambda fr: fr.id)
        for id, records in grouped:
            bucket = list(records)
            if not bucket:
                continue
            values = [self._extract_value(fr.record) for fr in bucket]
            mean, std = self._resolve_stats(id, values)
            for fr, raw in zip(bucket, values):
                normalized = raw
                if self.with_mean:
                    normalized -= mean
                if self.with_std:
                    normalized /= std
                yield FeatureRecord(
                    record=_clone_with_value(fr.record, normalized),
                    id=fr.id,
                )
