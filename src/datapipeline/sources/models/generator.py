from __future__ import annotations

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

    def __iter__(self) -> Iterator[Any]:
        return self.generate()

