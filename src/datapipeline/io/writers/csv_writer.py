import csv
from pathlib import Path
from typing import Optional

from datapipeline.io.serializers import csv_row_serializer, BaseCsvRowSerializer
from datapipeline.io.protocols import HasFilePath, Writer
from datapipeline.io.sinks import AtomicTextFileSink


class CsvFileWriter(Writer, HasFilePath):
    def __init__(self, dest: Path, serializer: BaseCsvRowSerializer | None = None):
        self.sink = AtomicTextFileSink(dest)
        self.writer = csv.writer(self.sink.fh)
        self.writer.writerow(["key", "values"])
        self._serializer = serializer or csv_row_serializer("sample")

    @property
    def file_path(self) -> Optional[Path]:
        return self.sink.file_path

    def write(self, item) -> None:
        self.writer.writerow(self._serializer(item))

    def close(self) -> None:
        self.sink.close()
