from abc import ABC, abstractmethod
from typing import Iterator, Any, Optional


class DataGenerator(ABC):
    """Base class for in-process data generators.

    Implements a uniform `generate()` iterator and optional `count()`.
    This intentionally separates generation from loading (I/O).
    """

    @abstractmethod
    def generate(self) -> Iterator[Any]:
        pass

    def count(self) -> Optional[int]:
        return None

    def info_lines(self) -> list[str]:
        return []

    def debug_lines(self) -> list[str]:
        return []

    def __iter__(self) -> Iterator[Any]:
        return self.generate()
