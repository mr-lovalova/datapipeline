from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any, TypeVar

from datapipeline.domain.record import TemporalRecord


class StreamTransformBase(ABC):
    """Base interface for stream transforms over TemporalRecord."""

    def __call__(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        return self.apply(stream)

    @abstractmethod
    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        ...


class FieldStreamTransformBase(StreamTransformBase):
    """Base for stream transforms that read/write a record field."""

    def __init__(
        self,
        field: str,
        to: str | None = None,
        partition_by: str | list[str] | None = None,
    ) -> None:
        if not field:
            raise ValueError("field is required")
        self.field = field
        self.to = to or field
        self.partition_by = partition_by

    def _ensure_output_field(
        self,
        record: TemporalRecord,
        value: Any = None,
    ) -> TemporalRecord:
        if self.to is None:
            return record
        if hasattr(record, self.to):
            return record
        setattr(record, self.to, value)
        return record


TRecord = TypeVar("TRecord", bound=TemporalRecord)


class RecordTransformBase(ABC):
    """Base interface for record transforms over TemporalRecord."""

    def __call__(self, stream: Iterator[TRecord]) -> Iterator[TRecord]:
        return self.apply(stream)

    @abstractmethod
    def apply(self, stream: Iterator[TRecord]) -> Iterator[TRecord]:
        ...
