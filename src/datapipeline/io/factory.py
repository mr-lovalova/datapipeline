from typing import Optional

from datapipeline.io.writers import (
    JsonLinesFileWriter,
    GzipJsonLinesWriter,
    CsvFileWriter,
    PickleFileWriter,
    LineWriter,
)
from datapipeline.io.protocols import Writer
from datapipeline.io.serializers import (
    json_line_serializer,
    csv_row_serializer,
    pickle_serializer,
    text_line_serializer,
)
from datapipeline.io.sinks import AtomicTextFileSink, StdoutTextSink
from datapipeline.io.output import OutputTarget

_JSON_LINE_FORMATS = {"jsonl"}


def _is_json_line_format(value: str) -> bool:
    return value in _JSON_LINE_FORMATS


def writer_factory(
    target: OutputTarget,
    *,
    visuals: Optional[str] = None,
) -> Writer:
    transport = target.transport.lower()
    format_ = target.format.lower()

    if transport == "stdout":
        sink = StdoutTextSink()
        if _is_json_line_format(format_):
            serializer = json_line_serializer(target.view)
            return LineWriter(sink, serializer)
        if format_ == "txt":
            serializer = text_line_serializer()
            return LineWriter(sink, serializer)
        raise ValueError(f"Unsupported stdout format '{target.format}'")

    destination = target.destination
    if destination is None:
        raise ValueError("fs output requires a destination path")
    destination.parent.mkdir(parents=True, exist_ok=True)
    text_encoding = target.encoding or "utf-8"

    suffix = "".join(destination.suffixes).lower()
    if _is_json_line_format(format_):
        serializer = json_line_serializer(target.view)
        if suffix.endswith(".jsonl.gz") or suffix.endswith(".gz"):
            return GzipJsonLinesWriter(destination, serializer=serializer, encoding=text_encoding)
        return JsonLinesFileWriter(destination, serializer=serializer, encoding=text_encoding)
    if format_ == "csv":
        serializer = csv_row_serializer(target.view)
        return CsvFileWriter(destination, serializer=serializer, encoding=text_encoding)
    if format_ == "pickle":
        serializer = pickle_serializer(target.view)
        return PickleFileWriter(destination, serializer=serializer)
    if format_ == "txt":
        serializer = text_line_serializer()
        sink = AtomicTextFileSink(destination, encoding=text_encoding)
        return LineWriter(sink, serializer)

    raise ValueError(f"Unsupported fs format '{target.format}'")
