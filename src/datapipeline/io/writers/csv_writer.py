import csv
from pathlib import Path
from typing import Optional

from datapipeline.io.formatters import CsvRowFormatter
from datapipeline.io.protocols import HasFilePath, Writer
from datapipeline.io.sinks import AtomicTextFileSink


class CsvFileWriter(Writer, HasFilePath):
    def __init__(self, dest: Path):
        self.sink = AtomicTextFileSink(dest)
        self.writer = csv.writer(self.sink.fh)
        self.writer.writerow(["key", "values"])
        self._fmt = CsvRowFormatter()

    @property
    def file_path(self) -> Optional[Path]:
        return self.sink.file_path

    def write(self, item) -> None:
        self.writer.writerow(self._fmt(item))

    def close(self) -> None:
        self.sink.close()
