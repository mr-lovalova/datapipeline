from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any, TypeVar

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.utils import partition_key


class StreamTransformBase(ABC):
    """Base interface for stream transforms over TemporalRecord."""

    def __call__(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        return self.apply(stream)

    @abstractmethod
    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        ...


class PartitionedStreamTransformBase(StreamTransformBase):
    """Base for stream transforms that operate per partition."""

    def __init__(
        self,
        partition_by: str | list[str] | None = None,
    ) -> None:
        self.partition_by = partition_by

    def partition_fields(self) -> tuple[str, ...]:
        if not self.partition_by:
            return ()
        if isinstance(self.partition_by, str):
            return (self.partition_by,)
        return tuple(self.partition_by)

    def partition_key(self, record: TemporalRecord) -> tuple:
        return partition_key(record, self.partition_by)


class PartitionedFieldStreamTransformBase(PartitionedStreamTransformBase):
    """Base for partitioned stream transforms that operate on one field."""

    def __init__(
        self,
        field: str,
        partition_by: str | list[str] | None = None,
    ) -> None:
        super().__init__(partition_by=partition_by)
        if not field:
            raise ValueError("field is required")
        self.field = field


class FieldValueStreamTransformBase(PartitionedFieldStreamTransformBase):
    """Base for stream transforms that read one field and write another."""

    def __init__(
        self,
        field: str,
        to: str | None = None,
        partition_by: str | list[str] | None = None,
    ) -> None:
        super().__init__(field=field, partition_by=partition_by)
        self.to = to or field

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
