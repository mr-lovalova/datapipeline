from typing import Iterator, Generic, TypeVar, Optional
from .base import SourceInterface
from .loader import RawDataLoader
from .parser import DataParser

TRecord = TypeVar("TRecord")


class Source(SourceInterface[TRecord], Generic[TRecord]):
    def __init__(self, loader: RawDataLoader, parser: DataParser[TRecord]):
        self.loader = loader
        self.parser = parser

    def stream(self) -> Iterator[TRecord]:
        for row in self.loader.load():
            try:
                parsed = self.parser.parse(row)
                if parsed is not None:
                    yield parsed
            except Exception:
                continue

    def count(self) -> Optional[int]:
        try:
            return self.loader.count()
        except Exception:
            return None

