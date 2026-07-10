from collections import deque
from itertools import groupby
from statistics import mean, median
from typing import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.interfaces import (
    FieldValueStreamTransformBase,
    StreamTransformBase,
)
from datapipeline.transforms.utils import (
    clone_record_with_field,
    get_field,
    is_missing,
)


class FillTransformer(StreamTransformBase):
    """Dispatch stream fill to the selected method."""

    def __init__(
        self,
        *,
        field: str,
        to: str | None = None,
        method: str = "median",
        window: int | None = None,
        min_samples: int | None = None,
        partition_by: str | list[str] | None = None,
    ) -> None:
        if method == "forward":
            if window is not None:
                raise ValueError("forward fill does not use window")
            if min_samples is not None:
                raise ValueError("forward fill does not use min_samples")
            self._impl = _ForwardFill(field=field, to=to, partition_by=partition_by)
            return

        if method in {"mean", "median"}:
            self._impl = _RollingFill(
                field=field,
                to=to,
                method=method,
                window=window,
                min_samples=min_samples,
                partition_by=partition_by,
            )
            return

        raise ValueError("fill method must be one of: 'median', 'mean', 'forward'")

    def bind_partition_by(
        self,
        partition_by: str | list[str] | None,
    ) -> None:
        self._impl.bind_partition_by(partition_by)

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        yield from self._impl.apply(stream)


class _RollingFill(FieldValueStreamTransformBase):
    def __init__(
        self,
        *,
        field: str,
        to: str | None,
        method: str,
        window: int | None,
        min_samples: int | None,
        partition_by: str | list[str] | None,
    ) -> None:
        super().__init__(field=field, to=to, partition_by=partition_by)
        if window is None or window <= 0:
            raise ValueError("window must be a positive integer")
        samples = 1 if min_samples is None else min_samples
        if samples <= 0:
            raise ValueError("min_samples must be positive")
        if method == "mean":
            self._aggregate = mean
        elif method == "median":
            self._aggregate = median
        else:
            raise ValueError("fill method must be one of: 'median', 'mean', 'forward'")

        self.window = window
        self.min_samples = samples

    def _compute_fill(self, values: list[float]) -> float | None:
        if not values:
            return None
        return float(self._aggregate(values))

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        grouped = groupby(stream, key=self.partition_key)

        for _, records in grouped:
            # Filled values are not fed back into the rolling window.
            tick_window: deque[tuple[bool, float | None]] = deque(maxlen=self.window)

            for record in records:
                value = get_field(record, self.field)
                record = self._ensure_output_field(
                    record, None if is_missing(value) else value
                )

                if is_missing(value):
                    valid_vals = [
                        num for valid, num in tick_window if valid and num is not None
                    ]
                    if len(valid_vals) >= self.min_samples:
                        fill = self._compute_fill(valid_vals)
                        if fill is not None:
                            tick_window.append((False, None))
                            yield clone_record_with_field(record, self.to, fill)
                            continue
                    # Not enough valid samples in window: pass through missing
                    tick_window.append((False, None))
                    yield record
                else:
                    as_float = float(value)
                    tick_window.append((True, as_float))
                    yield record


class _ForwardFill(FieldValueStreamTransformBase):
    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        grouped = groupby(stream, key=self.partition_key)

        for _, records in grouped:
            last_value = None
            has_value = False
            for record in records:
                value = get_field(record, self.field)
                if is_missing(value):
                    if has_value:
                        yield clone_record_with_field(record, self.to, last_value)
                    else:
                        yield self._ensure_output_field(record, None)
                    continue
                last_value = value
                has_value = True
                yield self._ensure_output_field(record, value)
