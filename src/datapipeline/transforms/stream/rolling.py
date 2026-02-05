from collections import deque
from itertools import groupby
from statistics import mean, median
from typing import Iterator

from datapipeline.domain.feature import FeatureRecord
from datapipeline.transforms.interfaces import FieldStreamTransformBase
from datapipeline.transforms.utils import (
    get_field,
    is_missing,
    clone_record_with_field,
)


class RollingTransformer(FieldStreamTransformBase):
    """Compute a rolling statistic over feature values.

    - window: number of recent ticks to consider (including missing ticks).
    - min_samples: minimum number of valid samples required to emit a value.
    - statistic: 'mean' (default) or 'median'.
    - field: record attribute to read (defaults to 'value').
    - to: record attribute to write (defaults to field).
    """

    def __init__(
        self,
        window: int,
        min_samples: int | None = None,
        statistic: str = "mean",
        field: str = "value",
        to: str | None = None,
    ) -> None:
        super().__init__(field=field, to=to)
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
        self.field = field
        self.to = to or field

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        grouped = groupby(stream, key=lambda fr: fr.id)

        for fid, feature_records in grouped:
            tick_window: deque[float | None] = deque(maxlen=self.window)

            for fr in feature_records:
                value = get_field(fr.record, self.field)
                if is_missing(value):
                    tick_window.append(None)
                else:
                    tick_window.append(float(value))

                valid_vals = [v for v in tick_window if v is not None]
                if len(valid_vals) >= self.min_samples:
                    rolled = float(self.statistic(valid_vals))
                else:
                    rolled = None

                record = clone_record_with_field(fr.record, self.to, rolled)
                yield FeatureRecord(record=record, id=fid)
