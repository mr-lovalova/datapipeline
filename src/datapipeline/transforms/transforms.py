from __future__ import annotations

from dataclasses import is_dataclass, replace
from datetime import timedelta
from itertools import groupby
from math import sqrt
from numbers import Real
from typing import Any, Iterator, Mapping, MutableMapping

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import Record, TimeFeatureRecord
from datapipeline.utils.time import parse_timecode


def _get_field(record: Any, field: str, default: Any = None) -> Any:
    """Retrieve attribute *field* from *record* supporting dicts and objects."""

    if isinstance(record, Mapping):
        return record.get(field, default)
    return getattr(record, field, default)


def _is_missing(value: Any) -> bool:
    """Return True when *value* should be treated as a missing observation."""

    if value is None:
        return True
    if isinstance(value, float):  # covers NaN/inf cases
        return value != value  # NaN check without importing numpy
    try:
        if isinstance(value, Real):
            return value != value
    except TypeError:
        pass
    return False


def _clone_with_value(record: Any, value: float) -> Any:
    """Return a shallow copy of *record* with its ``value`` field replaced."""

    if isinstance(record, list):
        raise TypeError(
            "StandardScalerTransform does not support sequence FeatureRecord payloads."
        )

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


def shift_record_time(record: TimeFeatureRecord, lag: timedelta) -> TimeFeatureRecord:
    record.time = record.time - lag
    return record


def time_lag(stream: Iterator[TimeFeatureRecord], lag: str) -> Iterator[TimeFeatureRecord]:
    lag_td = parse_timecode(lag)
    for record in stream:
        yield shift_record_time(record, lag_td)


def drop_missing_values(
    stream: Iterator[Any],
    field: str = "value",
) -> Iterator[Any]:
    """Filter out records whose *field* contains a missing/null value."""

    for record in stream:
        value = _get_field(record, field)
        if _is_missing(value):
            continue
        yield record


class StandardScalerTransform:
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

    def _extract_value(self, record: Record) -> float:
        value = _get_field(record, "value")
        if isinstance(value, Real):
            return float(value)
        raise TypeError(f"Record value must be numeric, got {value!r}")

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        grouped = groupby(stream, key=lambda fr: fr.feature_id)
        for feature_id, records in grouped:
            bucket = list(records)
            if not bucket:
                continue
            values = [self._extract_value(fr.record) for fr in bucket]
            mean, std = self._resolve_stats(feature_id, values)
            for fr, raw in zip(bucket, values):
                normalized = raw
                if self.with_mean:
                    normalized -= mean
                if self.with_std:
                    normalized /= std
                yield FeatureRecord(
                    record=_clone_with_value(fr.record, normalized),
                    feature_id=fr.feature_id,
                    group_key=fr.group_key,
                )
