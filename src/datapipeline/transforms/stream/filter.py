from collections.abc import Iterator
from typing import Any, Callable

from datapipeline.domain.feature import FeatureRecord
from datapipeline.filters import filters as _filters
from datapipeline.transforms.filter import apply_filter
from datapipeline.transforms.interfaces import StreamTransformBase


class FilterTransform(StreamTransformBase):
    """Filter feature records by comparing a field on record payloads."""

    def __init__(self, operator: str, field: str, comparand: Any) -> None:
        self.operator = operator
        self.field = field
        self.comparand = comparand

    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        return apply_filter(
            stream,
            field_getter=lambda fr, f: fr.id if f == "id" else _filters.get_field(fr.record, f),
            operator=self.operator,
            field=self.field,
            comparand=self.comparand,
        )

