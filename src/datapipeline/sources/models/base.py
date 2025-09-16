from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Iterator


TRecord = TypeVar("TRecord")


class SourceInterface(ABC, Generic[TRecord]):

    @abstractmethod
    def stream(self) -> Iterator[TRecord]:
        pass
