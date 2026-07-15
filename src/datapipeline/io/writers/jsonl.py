from pathlib import Path

from datapipeline.io.serializers import json_line_serializer
from datapipeline.io.sinks import AtomicTextFileSink

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
