from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import TypeVar

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord


class StreamTransformBase(ABC):
    """Base interface for stream transforms over FeatureRecord."""

    def __call__(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        return self.apply(stream)

    @abstractmethod
    def apply(self, stream: Iterator[FeatureRecord]) -> Iterator[FeatureRecord]:
        ...


class FieldStreamTransformBase(StreamTransformBase):
    """Base for stream transforms that read/write a record field."""

    def __init__(self, field: str = "value", to: str | None = None) -> None:
        self.field = field
        self.to = to or field


TRecord = TypeVar("TRecord", bound=TemporalRecord)


class RecordTransformBase(ABC):
    """Base interface for record transforms over TemporalRecord."""

    def __call__(self, stream: Iterator[TRecord]) -> Iterator[TRecord]:
        return self.apply(stream)

    @abstractmethod
    def apply(self, stream: Iterator[TRecord]) -> Iterator[TRecord]:
        ...
