from __future__ import annotations

from statistics import mean, median
from typing import Iterator

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.interfaces import FieldStreamTransformBase
from datapipeline.transforms.utils import (
    get_field,
    clone_record_with_field,
    partition_key,
)


class FeatureGranularityTransform(FieldStreamTransformBase):
    """Normalize same-timestamp duplicates for non-sequence streams.

    Single-argument API (preferred for concise YAML):
      - "first" | "last" | "mean" | "median" => aggregate duplicates within a timestamp.
    """

    def __init__(
        self,
        *,
        field: str,
        to: str | None = None,
        mode: str = "first",
        partition_by: str | list[str] | None = None,
    ) -> None:
        super().__init__(field=field, to=to, partition_by=partition_by)
        if mode not in {"first", "last", "mean", "median"}:
            raise ValueError(f"Unsupported granularity mode: {mode!r}")
        self.mode = mode

    def _aggregate(self, items: list[TemporalRecord]) -> TemporalRecord:
        vals: list[float] = []
        for rec in items:
            vals.append(float(get_field(rec, self.field)))
        if self.mode == "mean":
            agg_val = mean(vals)
        elif self.mode == "median":
            agg_val = median(vals)
        new = items[-1]
        return clone_record_with_field(new, self.to, agg_val)

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        """Aggregate duplicates per timestamp while preserving order.

        Precondition: input is sorted by (partition_key, record.time).

        We process one base stream at a time (partition_key),
        bucket its records by timestamp, then aggregate each bucket according to
        the selected mode (first/last/mean/median), emitting in increasing timestamp
        order.
        """

        # State for the current base stream: partition key
        current_key: tuple | None = None
        # Buckets of same-timestamp duplicates for the current base stream
        # Maintain insertion order of timestamps as encountered
        time_buckets: dict[object, list[TemporalRecord]] = {}

        def flush_current() -> Iterator[TemporalRecord]:
            if current_key is None or not time_buckets:
                return iter(())

            # Ordered list of timestamps as they appeared in the input
            ordered_times = list(time_buckets.keys())

            out: list[TemporalRecord] = []
            for t in ordered_times:
                bucket = time_buckets.get(t, [])
                if not bucket:
                    continue
                if self.mode == "last":
                    last = bucket[-1]
                    out.append(
                        clone_record_with_field(
                            last,
                            self.to,
                            get_field(last, self.field),
                        )
                    )
                elif self.mode == "first":
                    first = bucket[0]
                    out.append(
                        clone_record_with_field(
                            first,
                            self.to,
                            get_field(first, self.field),
                        )
                    )
                else:
                    out.append(self._aggregate(bucket))
            return iter(out)

        for record in stream:
            base_key = partition_key(record, self.partition_by)
            t = getattr(record, "time", None)
            # Start new base stream when partition key changes
            if current_key is not None and base_key != current_key:
                for out in flush_current():
                    yield out
                time_buckets = {}
            current_key = base_key
            # Append to the bucket for this timestamp
            bucket = time_buckets.get(t)
            if bucket is None:
                time_buckets[t] = [record]
            else:
                bucket.append(record)

        # Flush any remaining base stream
        if current_key is not None:
            for out in flush_current():
                yield out
