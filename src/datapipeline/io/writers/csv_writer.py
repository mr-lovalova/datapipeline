import csv
from pathlib import Path

from datapipeline.io.compression import Compression
from datapipeline.io.csv_projection import CsvTableProjector
from datapipeline.io.normalization import flat_payload
from datapipeline.io.sinks.files import AtomicTextFileSink


class CsvFileWriter:
    def __init__(
        self,
        dest: Path,
        encoding: str = "utf-8",
        overwrite: bool = True,
        compression: Compression | None = None,
    ) -> None:
        self.sink = AtomicTextFileSink(
            dest,
            encoding=encoding,
            overwrite=overwrite,
            newline="",
            compression=compression,
        )
        self.writer = csv.writer(self.sink.fh)
        self._header_written = False
        self._projector = CsvTableProjector(flat_payload)

    def write(self, item: object) -> None:
        projected = self._projector.project(item)
        if not self._header_written:
            self.writer.writerow(projected.header)
            self._header_written = True
        self.writer.writerow(projected.values)

    def close(self) -> None:
        self.sink.close()

    def abort(self) -> None:
        self.sink.abort()
