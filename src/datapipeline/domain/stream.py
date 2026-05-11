from collections.abc import Iterator
from typing import Protocol, TypeVar


TRecord = TypeVar("TRecord")


class RecordStream(Protocol[TRecord]):
    def stream(self) -> Iterator[TRecord]:
        pass
