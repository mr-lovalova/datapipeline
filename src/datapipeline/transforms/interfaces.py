from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any, Protocol, TypeVar, runtime_checkable

from datapipeline.dag.context import PipelineContext
from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.utils import partition_key


@runtime_checkable
class SupportsContextBinding(Protocol):
    def bind_context(self, context: PipelineContext) -> None: ...


@runtime_checkable
class SupportsPartitionBinding(Protocol):
    def bind_partition_by(
        self,
        partition_by: str | list[str] | None,
    ) -> None: ...


class StreamTransformBase(ABC):
    """Base interface for stream transforms over TemporalRecord."""

    def __call__(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        return self.apply(stream)

    @abstractmethod
    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]: ...


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

    def bind_partition_by(
        self,
        partition_by: str | list[str] | None,
    ) -> None:
        if partition_by is None:
            return
        stream_fields = (
            (partition_by,) if isinstance(partition_by, str) else tuple(partition_by)
        )
        configured_fields = self.partition_fields()
        if configured_fields and configured_fields != stream_fields:
            raise ValueError(
                "Transform partition_by must match the stream partition_by; "
                "set partition_by on the stream instead of inside the transform."
            )
        self.partition_by = partition_by

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
    def apply(self, stream: Iterator[TRecord]) -> Iterator[TRecord]: ...
