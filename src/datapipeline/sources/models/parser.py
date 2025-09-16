from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

TRecord = TypeVar("TRecord")


class DataParser(ABC, Generic[TRecord]):

    @abstractmethod
    def parse(self, raw: Any) -> TRecord:
        pass
