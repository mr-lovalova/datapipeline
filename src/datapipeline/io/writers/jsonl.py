from pathlib import Path

from datapipeline.io.normalization import View
from datapipeline.io.serializers import json_line_serializer
from datapipeline.io.sinks.files import AtomicTextFileSink

from .base import LineWriter


class JsonLinesFileWriter(LineWriter):
    def __init__(
        self,
        dest: Path,
        view: View = "raw",
        encoding: str = "utf-8",
        overwrite: bool = True,
    ) -> None:
        sink = AtomicTextFileSink(
            dest,
            encoding=encoding,
            overwrite=overwrite,
        )
        super().__init__(sink, json_line_serializer(view))
