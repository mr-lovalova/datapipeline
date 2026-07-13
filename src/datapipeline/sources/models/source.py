from typing import Iterator, Generic, TypeVar
from datapipeline.domain.stream import RecordStream
from .loader import BaseDataLoader
from .parser import DataParser
from .parsing_error import ParsingError

TRecord = TypeVar("TRecord")


class Source(RecordStream[TRecord], Generic[TRecord]):
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
