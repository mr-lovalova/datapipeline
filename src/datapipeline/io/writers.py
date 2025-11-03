from typing import Optional
from pathlib import Path
import json
import csv
import pickle

from datapipeline.io.sinks import (
    StdoutTextSink,
    AtomicTextFileSink,
    GzipBinarySink,
    AtomicBinaryFileSink,
)
from datapipeline.io.protocols import Writer, HeaderCapable, HasFilePath
from datapipeline.io.formatters import JsonLineFormatter, CsvRowFormatter, PickleFormatter


class LineWriter(Writer):
    """Text line writer (uses a text sink + string formatter)."""

    def __init__(self, sink: StdoutTextSink | AtomicTextFileSink, formatter):
        self.sink = sink
        self.fmt = formatter

    def write(self, rec: dict) -> None:
        self.sink.write_text(self.fmt(rec))

    def close(self) -> None:
        self.sink.close()


class HeaderJsonlMixin(HeaderCapable):
    """Provide a header write by emitting one JSON line."""

    def write_header(self, header: dict) -> None:
        self.sink.write_text(
            json.dumps({"__checkpoint__": header}, ensure_ascii=False) + "\n"
        )


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

    @property
    def file_path(self) -> Optional[Path]:
        return self.sink.file_path

    def write_header(self, header: dict) -> None:
        self.sink.write_bytes(
            (json.dumps({"__checkpoint__": header},
             ensure_ascii=False) + "\n").encode("utf-8")
        )

    def write(self, rec: dict) -> None:
        self.sink.write_bytes(
            (json.dumps(rec, ensure_ascii=False, default=str) + "\n").encode("utf-8")
        )

    def close(self) -> None:
        self.sink.close()


class CsvFileWriter(Writer, HasFilePath):
    def __init__(self, dest: Path):
        self.sink = AtomicTextFileSink(dest)
        self.writer = csv.writer(self.sink.fh)  # reuse the sink's file handle
        self.writer.writerow(["key", "values"])
        self._fmt = CsvRowFormatter()

    @property
    def file_path(self) -> Optional[Path]:
        return self.sink.file_path

    def write(self, rec: dict) -> None:
        self.writer.writerow(self._fmt(rec))

    def close(self) -> None:
        self.sink.close()


class PickleFileWriter(Writer, HasFilePath):
    def __init__(self, dest: Path, protocol: int = pickle.HIGHEST_PROTOCOL):
        self.sink = AtomicBinaryFileSink(dest)
        # type: ignore[attr-defined]
        self.pickler = pickle.Pickler(self.sink.fh, protocol=protocol)
        self._fmt = PickleFormatter()

    @property
    def file_path(self) -> Optional[Path]:
        return self.sink.file_path

    def write(self, rec: dict) -> None:
        self.pickler.dump(self._fmt(rec))

    def close(self) -> None:
        self.sink.close()
