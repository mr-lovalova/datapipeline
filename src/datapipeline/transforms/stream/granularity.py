from __future__ import annotations

from statistics import mean, median
from typing import Iterator

from datapipeline.domain.feature import FeatureRecord


class FeatureGranularityTransform:
    """Normalize same-timestamp duplicates for non-sequence features.

    Single-argument API (preferred for concise YAML):
      - "first" | "last" | "mean" | "median" => aggregate duplicates within a timestamp.
    """

    def __init__(self, mode: str = "first") -> None:
        if mode not in {"first", "last", "mean", "median"}:
            raise ValueError(f"Unsupported granularity mode: {mode!r}")
        self.mode = mode

    def _aggregate(self, items: list[FeatureRecord]) -> FeatureRecord:
        vals: list[float] = []
        for fr in items:
            vals.append(float(fr.record.value))
        if self.mode == "mean":
            agg_val = mean(vals)
        elif self.mode == "median":
            agg_val = median(vals)
        new = items[-1]
        new.record.value = agg_val
        return new

    def __call__(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        return self.apply(stream)

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        """Aggregate duplicates per timestamp while preserving order.

        Precondition: input is sorted by (feature_id, record.time).

        We process one base feature stream at a time (feature_id),
        bucket its records by timestamp, then aggregate each bucket according to
        the selected mode (first/last/mean/median), emitting in increasing timestamp
        order.
        """

        # State for the current base stream: id
        current_key: str | None = None
        # Buckets of same-timestamp duplicates for the current base stream
        # Maintain insertion order of timestamps as encountered
        time_buckets: dict[object, list[FeatureRecord]] = {}

        def flush_current() -> Iterator[FeatureRecord]:
            if current_key is None or not time_buckets:
                return iter(())

            # Ordered list of timestamps as they appeared in the input
            ordered_times = list(time_buckets.keys())

            out: list[FeatureRecord] = []
            for t in ordered_times:
                bucket = time_buckets.get(t, [])
                if not bucket:
                    continue
                if self.mode == "last":
                    out.append(bucket[-1])
                elif self.mode == "first":
                    out.append(bucket[0])
                else:
                    out.append(self._aggregate(bucket))
            return iter(out)

        for fr in stream:
            base_key = fr.id
            t = getattr(fr.record, "time", None)
            # Start new base stream when feature_id changes
            if current_key is not None and base_key != current_key:
                for out in flush_current():
                    yield out
                time_buckets = {}
            current_key = base_key
            # Append to the bucket for this timestamp
            bucket = time_buckets.get(t)
            if bucket is None:
                time_buckets[t] = [fr]
            else:
                bucket.append(fr)

        # Flush any remaining base stream
        if current_key is not None:
            for out in flush_current():
                yield out
