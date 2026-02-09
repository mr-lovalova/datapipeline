from pathlib import Path
from typing import Optional
import json

from datapipeline.io.serializers import json_line_serializer
from datapipeline.io.protocols import HeaderCapable, HasFilePath, Writer
from datapipeline.io.sinks import (
    AtomicTextFileSink,
    GzipBinarySink,
    StdoutTextSink,
)

from .base import HeaderJsonlMixin, LineWriter


class JsonLinesStdoutWriter(LineWriter, HeaderJsonlMixin):
    def __init__(self, serializer=None):
        super().__init__(StdoutTextSink(), serializer or json_line_serializer())


class JsonLinesFileWriter(LineWriter, HeaderJsonlMixin, HasFilePath):
    def __init__(self, dest: Path, serializer=None, encoding: str = "utf-8"):
        self._sink = AtomicTextFileSink(dest, encoding=encoding)
        super().__init__(self._sink, serializer or json_line_serializer())

    @property
    def file_path(self) -> Optional[Path]:
        return self._sink.file_path


class GzipJsonLinesWriter(Writer, HeaderCapable, HasFilePath):
    def __init__(self, dest: Path, serializer=None, encoding: str = "utf-8"):
        self.sink = GzipBinarySink(dest)
        self._serializer = serializer or json_line_serializer()
        self._encoding = encoding

    @property
    def file_path(self) -> Optional[Path]:
        return self.sink.file_path

    def write_header(self, header: dict) -> None:
        self.sink.write_bytes(
            (json.dumps({"__checkpoint__": header}, ensure_ascii=False) + "\n").encode(
                self._encoding
            )
        )

    def write(self, item) -> None:
        line = self._serializer(item)
        self.sink.write_bytes(line.encode(self._encoding))

    def close(self) -> None:
        self.sink.close()
