from collections.abc import Iterator
from typing import Any

from datapipeline.domain.record import TemporalRecord
from datapipeline.filters import filters as _filters
from datapipeline.transforms.where import apply_where
from datapipeline.transforms.interfaces import StreamTransformBase


class WhereTransform(StreamTransformBase):
    """Keep records where a field comparison is true."""

    def __init__(self, operator: str, field: str, comparand: Any) -> None:
        self.operator = operator
        self.field = field
        self.comparand = comparand

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        return apply_where(
            stream,
            field_getter=_filters.get_field,
            operator=self.operator,
            field=self.field,
            comparand=self.comparand,
        )
