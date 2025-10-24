from itertools import groupby
from statistics import mean, median
from typing import Any, Iterator
from collections import deque

from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.transforms.utils import is_missing, clone_record_with_value


def _extract_value(record: Any) -> Any:
    return getattr(record, "value", None)


class FillTransformer:
    """Time-aware imputer using a strict rolling tick window.

    - window: number of recent ticks to consider (including missing ticks). A
      fill value is produced for a missing tick only if at least
      `min_samples` valid (non-missing) values exist within the last `window`
      ticks.
    - min_samples: minimum number of valid values required in the window.
    - statistic: 'median' (default) or 'mean' over the valid values in the
      window.
    """

    def __init__(self, statistic: str = "median", window: int | None = None, min_samples: int = 1) -> None:
        if window is None or window <= 0:
            raise ValueError("window must be a positive integer")
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

    def _compute_fill(self, values: list[float]) -> float | None:
        if not values:
            return None
        return float(self.statistic(values))

    def __call__(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecordSequence]:
        return self.apply(stream)

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecordSequence]:
        grouped = groupby(stream, key=lambda fr: fr.id)

        for id, feature_records in grouped:
            # Store the last `window` ticks with a flag marking whether the tick
            # had an original (non-filled) valid value, and its numeric value.
            tick_window: deque[tuple[bool, float | None]] = deque(maxlen=self.window)

            for fr in feature_records:
                if isinstance(fr.record, FeatureRecordSequence):
                    raise TypeError("Fills should run before windowing transforms")
                value = _extract_value(fr.record)

                if is_missing(value):
                    # Count valid values in the current window
                    valid_vals = [num for valid, num in tick_window if valid and num is not None]
                    if len(valid_vals) >= self.min_samples:
                        fill = self._compute_fill(valid_vals)
                        if fill is not None:
                            # Do NOT treat filled value as original valid; append a missing marker
                            tick_window.append((False, None))
                            yield FeatureRecord(
                                record=clone_record_with_value(fr.record, fill),
                                id=id,
                            )
                            continue
                    # Not enough valid samples in window: pass through missing
                    tick_window.append((False, None))
                    yield fr
                else:
                    as_float = float(value)
                    tick_window.append((True, as_float))
                    yield fr
