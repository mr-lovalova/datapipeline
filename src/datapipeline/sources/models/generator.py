from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any


class DataGenerator(ABC):
    """Generate in-process source rows."""

    @abstractmethod
    def generate(self) -> Iterator[Any]: ...

    def count(self) -> int | None:
        return None

    def __iter__(self) -> Iterator[Any]:
        return self.generate()
