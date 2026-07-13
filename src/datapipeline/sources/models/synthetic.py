from abc import ABC
from typing import TypeVar

from datapipeline.domain.stream import RecordStream

TRecord = TypeVar("TRecord")


class GenerativeSourceInterface(RecordStream[TRecord], ABC):
    """Marker interface - use if source doesn't rely on external data."""
    pass
