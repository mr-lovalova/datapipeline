from __future__ import annotations

from collections.abc import Iterator
from typing import Callable, TypeVar

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord

TRecord = TypeVar("TRecord", bound=TemporalRecord)


def map_record_stream(
    stream: Iterator[FeatureRecord],
    fn: Callable[[TRecord], TRecord],
) -> Iterator[FeatureRecord]:
    """Apply a per-record mapping function while preserving feature id."""
    for fr in stream:
        yield FeatureRecord(record=fn(fr.record), id=fr.id)
