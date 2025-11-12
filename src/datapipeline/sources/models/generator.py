from abc import ABC, abstractmethod
from typing import Iterator, Any, Optional
import logging


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


logger = logging.getLogger(__name__)


class NoOpGenerator(DataGenerator):
    """A data generator that yields no items, logging on consumption.

    Optionally accepts a custom message for clarity when constructed.
    """

    def __init__(self, message: Optional[str] = None, *, level: int = logging.WARNING):
        self._message = message
        self._level = level
        self._warned = False

    def _warn_once(self) -> None:
        if not self._warned:
            msg = self._message or "NoOpGenerator consumed; yielding no data"
            logger.log(self._level, msg)
            self._warned = True

    def generate(self) -> Iterator[Any]:
        self._warn_once()
        return iter(())

    def count(self) -> Optional[int]:
        self._warn_once()
        return 0
