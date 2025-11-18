from typing import Iterator, Generic, TypeVar, Optional
from .base import SourceInterface
from .loader import BaseDataLoader
from .parser import DataParser
from .parsing_error import ParsingError

TRecord = TypeVar("TRecord")


class Source(SourceInterface[TRecord], Generic[TRecord]):
    def __init__(self, loader: BaseDataLoader, parser: DataParser[TRecord]):
        self.loader = loader
        self.parser = parser

    def stream(self) -> Iterator[TRecord]:
        for i, row in enumerate(self.loader.load()):
            try:
                parsed = self.parser.parse(row)
                if parsed is not None:
                    yield parsed
            except Exception as exc:
                raise ParsingError(row=row, index=i, original_exc=exc) from exc

    def count(self) -> Optional[int]:
        try:
            return self.loader.count()
        except Exception:
            return None
