from pathlib import Path

from datapipeline.io.serializers import json_line_serializer
from datapipeline.io.sinks import (
    AtomicTextFileSink,
    GzipBinarySink,
)

from .base import LineWriter


class JsonLinesFileWriter(LineWriter):
    def __init__(
        self,
        dest: Path,
        serializer=None,
        encoding: str = "utf-8",
        overwrite: bool = True,
    ):
        self._sink = AtomicTextFileSink(
            dest,
            encoding=encoding,
            overwrite=overwrite,
        )
        super().__init__(self._sink, serializer or json_line_serializer())


class GzipJsonLinesWriter:
    def __init__(
        self,
        dest: Path,
        serializer=None,
        encoding: str = "utf-8",
        overwrite: bool = True,
    ):
        self.sink = GzipBinarySink(dest, overwrite=overwrite)
        self._serializer = serializer or json_line_serializer()
        self._encoding = encoding

    def write(self, item) -> None:
        line = self._serializer(item)
        self.sink.write_bytes(line.encode(self._encoding))

    def close(self) -> None:
        self.sink.close()

    def abort(self) -> None:
        self.sink.abort()
