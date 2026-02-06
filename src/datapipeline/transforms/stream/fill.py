from collections import deque
from itertools import groupby
from statistics import mean, median
from typing import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.interfaces import FieldStreamTransformBase
from datapipeline.transforms.utils import (
    get_field,
    is_missing,
    clone_record_with_field,
    partition_key,
)


class FillTransformer(FieldStreamTransformBase):
    """Time-aware imputer using a strict rolling tick window.

    - window: number of recent ticks to consider (including missing ticks). A
      fill value is produced for a missing tick only if at least
      `min_samples` valid (non-missing) values exist within the last `window`
      ticks.
    - min_samples: minimum number of valid values required in the window.
    - statistic: 'median' (default) or 'mean' over the valid values in the
      window.
    """

    def __init__(
        self,
        *,
        field: str,
        to: str | None = None,
        statistic: str = "median",
        window: int | None = None,
        min_samples: int = 1,
        partition_by: str | list[str] | None = None,
    ) -> None:
        super().__init__(field=field, to=to, partition_by=partition_by)
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

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        grouped = groupby(stream, key=lambda rec: partition_key(rec, self.partition_by))

        for _, records in grouped:
            # Store the last `window` ticks with a flag marking whether the tick
            # had an original (non-filled) valid value, and its numeric value.
            tick_window: deque[tuple[bool, float | None]] = deque(maxlen=self.window)

            for record in records:
                value = get_field(record, self.field)
                record = self._ensure_output_field(
                    record, None if is_missing(value) else value
                )

                if is_missing(value):
                    # Count valid values in the current window
                    valid_vals = [num for valid, num in tick_window if valid and num is not None]
                    if len(valid_vals) >= self.min_samples:
                        fill = self._compute_fill(valid_vals)
                        if fill is not None:
                            # Do NOT treat filled value as original valid; append a missing marker
                            tick_window.append((False, None))
                            yield clone_record_with_field(
                                record, self.to, fill
                            )
                            continue
                    # Not enough valid samples in window: pass through missing
                    tick_window.append((False, None))
                    yield record
                else:
                    as_float = float(value)
                    tick_window.append((True, as_float))
                    yield record
