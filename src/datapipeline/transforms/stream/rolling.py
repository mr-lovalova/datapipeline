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


class RollingTransformer(FieldStreamTransformBase):
    """Compute a rolling statistic over record field values.

    - window: number of recent ticks to consider (including missing ticks).
    - min_samples: minimum number of valid samples required to emit a value.
    - statistic: 'mean' (default) or 'median'.
    - field: record attribute to read.
    - to: record attribute to write (defaults to field).
    """

    def __init__(
        self,
        *,
        field: str,
        to: str | None = None,
        window: int,
        min_samples: int | None = None,
        statistic: str = "mean",
        partition_by: str | list[str] | None = None,
    ) -> None:
        super().__init__(field=field, to=to, partition_by=partition_by)
        if window <= 0:
            raise ValueError("window must be a positive integer")
        if min_samples is None:
            min_samples = window
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

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        grouped = groupby(stream, key=lambda rec: partition_key(rec, self.partition_by))

        for _, records in grouped:
            tick_window: deque[float | None] = deque(maxlen=self.window)

            for record in records:
                value = get_field(record, self.field)
                if is_missing(value):
                    tick_window.append(None)
                else:
                    tick_window.append(float(value))

                valid_vals = [v for v in tick_window if v is not None]
                if len(valid_vals) >= self.min_samples:
                    rolled = float(self.statistic(valid_vals))
                else:
                    rolled = None

                yield clone_record_with_field(record, self.to, rolled)
