from __future__ import annotations

from collections import deque
from dataclasses import is_dataclass, replace
from itertools import groupby
from statistics import mean, median
from typing import Any, Iterator, Mapping, MutableMapping

from datapipeline.domain.feature import FeatureRecord


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        return value != value  # NaN without numpy
    return False


def _extract_value(record: Any) -> Any:
    if isinstance(record, Mapping):
        return record.get("value")
    return getattr(record, "value", None)


def _clone_with_value(record: Any, value: float) -> Any:
    if isinstance(record, MutableMapping):
        cloned = type(record)(record)
        cloned["value"] = value
        return cloned
    if isinstance(record, Mapping):
        cloned = dict(record)
        cloned["value"] = value
        return cloned
    if hasattr(record, "value"):
        if is_dataclass(record):
            return replace(record, value=value)
        clone = type(record)(**record.__dict__)
        clone.value = value
        return clone
    raise TypeError(f"Unsupported record type for fill transform: {type(record)!r}")


class TimeWindowTransformer:
    def __init__(self, size: int, stride: int = 1) -> None:
        self.size = size
        self.stride = stride

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        """Assumes input is pre-sorted by (feature_id, record.time).

        Produces sliding windows per feature_id. Each output FeatureRecord has
        a list[Record] in ``record`` and carries the current group_key.
        """

        grouped = groupby(stream, key=lambda fr: fr.feature_id)

        for feature_id, records in grouped:
            window = deque(maxlen=self.size)
            step = 0
            for fr in records:
                window.append(fr)
                if len(window) == self.size and step % self.stride == 0:
                    yield FeatureRecord(
                        record=[r.record for r in window],
                        feature_id=feature_id,
                        group_key=fr.group_key,
                    )
                step += 1


class _BaseTimeFillTransformer:
    """Shared implementation for time-aware imputers."""

    def __init__(self, window: int | None = None, min_samples: int = 1) -> None:
        if window is not None and window <= 0:
            raise ValueError("window must be positive when provided")
        if min_samples <= 0:
            raise ValueError("min_samples must be positive")

        self.window = window
        self.min_samples = min_samples

    def _compute_fill(self, history: list[float]) -> float | None:
        raise NotImplementedError

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        grouped = groupby(stream, key=lambda fr: fr.feature_id)

        for feature_id, records in grouped:
            history: list[float] = []
            for fr in records:
                value = _extract_value(fr.record)
                if _is_missing(value):
                    if len(history) >= self.min_samples:
                        fill = self._compute_fill(history)
                        if fill is not None:
                            yield FeatureRecord(
                                record=_clone_with_value(fr.record, fill),
                                feature_id=feature_id,
                                group_key=fr.group_key,
                            )
                            continue
                    yield fr
                else:
                    as_float = float(value)
                    history.append(as_float)
                    if self.window is not None and len(history) > self.window:
                        history.pop(0)
                    yield fr


class TimeMeanFillTransformer(_BaseTimeFillTransformer):
    """Fill missing values using the running mean of prior observations."""

    def _compute_fill(self, history: list[float]) -> float | None:
        if not history:
            return None
        return float(mean(history))


class TimeMedianFillTransformer(_BaseTimeFillTransformer):
    """Fill missing values using the running median of prior observations."""

    def _compute_fill(self, history: list[float]) -> float | None:
        if not history:
            return None
        return float(median(history))
