from typing import Optional

from datapipeline.io.writers import (
    JsonLinesFileWriter,
    JsonLinesStdoutWriter,
    GzipJsonLinesWriter,
    CsvFileWriter,
    PickleFileWriter,
    LineWriter,
)
from datapipeline.io.protocols import Writer
from datapipeline.io.serializers import (
    json_line_serializer,
    print_serializer,
    csv_row_serializer,
    pickle_serializer,
)
from datapipeline.io.sinks import StdoutTextSink, RichStdoutSink, ReprRichFormatter, JsonRichFormatter, PlainRichFormatter
from datapipeline.io.output import OutputTarget

_JSON_LINE_FORMATS = {"jsonl"}


def _is_json_line_format(value: str) -> bool:
    return value in _JSON_LINE_FORMATS


def stdout_sink_for(format_: str, visuals: Optional[str]) -> StdoutTextSink:
    """Select an appropriate stdout sink given format and visuals preference.

    Behavior:
    - visuals == "rich" or "auto" -> attempt Rich formatting; fallback to plain on error.
    - anything else               -> plain stdout (no Rich formatting).
    """
    fmt = (format_ or "print").lower()
    provider = (visuals or "auto").lower()

    use_rich = provider == "rich" or provider == "auto"
    if not use_rich:
        return StdoutTextSink()

    # Prefer Rich when possible; gracefully degrade to plain stdout on any failure.
    try:
        if _is_json_line_format(fmt):
            return RichStdoutSink(JsonRichFormatter())
        if fmt == "print":
            return RichStdoutSink(ReprRichFormatter())
        return RichStdoutSink(PlainRichFormatter())
    except Exception:
        return StdoutTextSink()


def writer_factory(
    target: OutputTarget,
    *,
    visuals: Optional[str] = None,
    item_type: str = "sample",
) -> Writer:
    transport = target.transport.lower()
    format_ = target.format.lower()

    if item_type not in {"sample", "record"}:
        raise ValueError(f"Unsupported writer item_type '{item_type}'")

    if transport == "stdout":
        sink = stdout_sink_for(format_, visuals)
        if _is_json_line_format(format_):
            serializer = json_line_serializer(item_type, target.view)
            return LineWriter(sink, serializer)
        if format_ == "print":
            serializer = print_serializer(item_type, target.view)
            return LineWriter(sink, serializer)
        raise ValueError(f"Unsupported stdout format '{target.format}'")

    destination = target.destination
    if destination is None:
        raise ValueError("fs output requires a destination path")
    destination.parent.mkdir(parents=True, exist_ok=True)
    text_encoding = target.encoding or "utf-8"

    suffix = "".join(destination.suffixes).lower()
    if _is_json_line_format(format_):
        serializer = json_line_serializer(item_type, target.view)
        if suffix.endswith(".jsonl.gz") or suffix.endswith(".gz"):
            return GzipJsonLinesWriter(destination, serializer=serializer, encoding=text_encoding)
        return JsonLinesFileWriter(destination, serializer=serializer, encoding=text_encoding)
    if format_ == "csv":
        serializer = csv_row_serializer(item_type, target.view)
        return CsvFileWriter(destination, serializer=serializer, encoding=text_encoding)
    if format_ == "pickle":
        serializer = pickle_serializer(item_type, target.view)
        return PickleFileWriter(destination, serializer=serializer)

    raise ValueError(f"Unsupported fs format '{target.format}'")
