import csv
from pathlib import Path

from datapipeline.io.csv_projection import CsvTableProjector
from datapipeline.io.serializers import csv_row_serializer
from datapipeline.io.sinks import AtomicTextFileSink


class CsvFileWriter:
    def __init__(
        self,
        dest: Path,
        serializer=None,
        encoding: str = "utf-8",
        overwrite: bool = True,
    ):
        self.sink = AtomicTextFileSink(
            dest,
            encoding=encoding,
            overwrite=overwrite,
        )
        self.writer = csv.writer(self.sink.fh)
        self._header_written = False
        row_projector = serializer or csv_row_serializer()
        self._projector = CsvTableProjector(row_projector)

    def write(self, item) -> None:
        projected = self._projector.project(item)
        if not self._header_written:
            self.writer.writerow(projected.header)
            self._header_written = True
        self.writer.writerow(projected.values)

    def close(self) -> None:
        self.sink.close()

    def abort(self) -> None:
        self.sink.abort()
