from abc import ABC
from typing import TypeVar

from datapipeline.sources.models.base import SourceInterface

TRecord = TypeVar("TRecord")


class GenerativeSourceInterface(SourceInterface[TRecord], ABC):
    """Marker interface – use if source doesn't rely on external data."""
    pass
