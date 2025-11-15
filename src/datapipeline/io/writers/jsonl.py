from pathlib import Path
from typing import Optional
import json

from datapipeline.io.formatters import JsonLineFormatter
from datapipeline.io.protocols import HeaderCapable, HasFilePath, Writer
from datapipeline.io.sinks import (
    AtomicTextFileSink,
    GzipBinarySink,
    StdoutTextSink,
)

from .base import HeaderJsonlMixin, LineWriter


class JsonLinesStdoutWriter(LineWriter, HeaderJsonlMixin):
    def __init__(self):
        super().__init__(StdoutTextSink(), JsonLineFormatter())


class JsonLinesFileWriter(LineWriter, HeaderJsonlMixin, HasFilePath):
    def __init__(self, dest: Path):
        self._sink = AtomicTextFileSink(dest)
        super().__init__(self._sink, JsonLineFormatter())

    @property
    def file_path(self) -> Optional[Path]:
        return self._sink.file_path


class GzipJsonLinesWriter(Writer, HeaderCapable, HasFilePath):
    def __init__(self, dest: Path):
        self.sink = GzipBinarySink(dest)
        self._fmt = JsonLineFormatter()

    @property
    def file_path(self) -> Optional[Path]:
        return self.sink.file_path

    def write_header(self, header: dict) -> None:
        self.sink.write_bytes(
            (json.dumps({"__checkpoint__": header}, ensure_ascii=False) + "\n").encode(
                "utf-8"
            )
        )

    def write(self, item) -> None:
        line = self._fmt(item)
        self.sink.write_bytes(line.encode("utf-8"))

    def close(self) -> None:
        self.sink.close()
