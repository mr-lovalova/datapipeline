from __future__ import annotations

from collections.abc import Iterator

from datapipeline.domain.record import TemporalRecord


class FeatureDeduplicateTransform:
    """Drop consecutive identical records (timestamp + payload)."""

    def __init__(self, **_: object) -> None:
        # Accept arbitrary config mapping for consistency with other transforms.
        pass

    def __call__(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        return self.apply(stream)

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        last: TemporalRecord | None = None
        for record in stream:
            if last is not None and record == last:
                continue
            last = record
            yield record
