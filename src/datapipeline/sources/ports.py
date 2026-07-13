from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Iterator


@dataclass(frozen=True)
class SourceResource:
    uri: str | None
    stream: Iterable[bytes]


class SourceTransport(ABC):
    """Input transport that yields raw-byte resources."""

    @abstractmethod
    def resources(self) -> Iterator[SourceResource]:
        pass
