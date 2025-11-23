from __future__ import annotations

from collections.abc import Iterator

from datapipeline.domain.feature import FeatureRecord


class FeatureDeduplicateTransform:
    """Drop consecutive identical feature records (id + timestamp + payload)."""

    def __init__(self, **_: object) -> None:
        # Accept arbitrary config mapping for consistency with other transforms.
        pass

    def __call__(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        return self.apply(stream)

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        last: FeatureRecord | None = None
        for record in stream:
            if last is not None and record == last:
                continue
            last = record
            yield record
