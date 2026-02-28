from abc import ABC, abstractmethod
from typing import Iterable, Iterator


class SourceTransport(ABC):
    """Input transport that yields raw-byte streams per resource."""

    @abstractmethod
    def streams(self) -> Iterator[Iterable[bytes]]:
        pass
