from dataclasses import is_dataclass, replace
from itertools import groupby
from statistics import mean, median
from typing import Any, Iterator, Mapping, MutableMapping

from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.transforms.utils import is_missing


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
    raise TypeError(
        f"Unsupported record type for fill transform: {type(record)!r}")


class FillTransformer:
    """Shared implementation for time-aware imputers."""

    def __init__(self, statistic="median", window: int | None = None, min_samples: int = 1) -> None:

        if window is not None and window <= 0:
            raise ValueError("window must be positive when provided")
        if min_samples <= 0:
            raise ValueError("min_samples must be positive")
        if statistic == "mean":
            self.statistic = mean
        elif statistic == "median":
            self.statistic = median
        else:
            raise ValueError(f"Unsupported statistic: {statistic!r}")

        self.window = window
        self.min_samples = min_samples

    def _compute_fill(self, history: list[float]) -> float | None:
        if not history:
            return None
        return float(self.statistic(history))

    def __call__(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecordSequence]:
        return self.apply(stream)

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecordSequence]:
        grouped = groupby(stream, key=lambda fr: fr.id)

        for id, feature_records in grouped:
            history: list[float] = []
            for fr in feature_records:
                if isinstance(fr.record, FeatureRecordSequence):
                    raise TypeError(
                        "Fills should run before windowing transforms"
                    )
                value = _extract_value(fr.record)
                if is_missing(value):
                    if len(history) >= self.min_samples:
                        fill = self._compute_fill(history)
                        if fill is not None:
                            yield FeatureRecord(
                                record=_clone_with_value(fr.record, fill),
                                id=id,
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
