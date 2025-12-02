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
    record_json_line_serializer,
    record_print_serializer,
    record_csv_row_serializer,
    record_pickle_serializer,
)
from datapipeline.io.sinks import StdoutTextSink, RichStdoutSink, ReprRichFormatter, JsonRichFormatter, PlainRichFormatter
from datapipeline.io.output import OutputTarget


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
        if fmt in {"json", "json-lines", "jsonl"}:
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
    payload = target.payload

    if item_type not in {"sample", "record"}:
        raise ValueError(f"Unsupported writer item_type '{item_type}'")

    if transport == "stdout":
        sink = stdout_sink_for(format_, visuals)
        if format_ in {"json-lines", "json", "jsonl"}:
            serializer = (
                record_json_line_serializer()
                if item_type == "record"
                else json_line_serializer(payload)
            )
            return LineWriter(sink, serializer)
        if format_ == "print":
            serializer = (
                record_print_serializer()
                if item_type == "record"
                else print_serializer(payload)
            )
            return LineWriter(sink, serializer)
        raise ValueError(f"Unsupported stdout format '{target.format}'")

    destination = target.destination
    if destination is None:
        raise ValueError("fs output requires a destination path")
    destination.parent.mkdir(parents=True, exist_ok=True)

    suffix = "".join(destination.suffixes).lower()
    if format_ in {"json-lines", "json", "jsonl"}:
        serializer = (
            record_json_line_serializer()
            if item_type == "record"
            else json_line_serializer(payload)
        )
        if suffix.endswith(".jsonl.gz") or suffix.endswith(".json.gz") or suffix.endswith(".gz"):
            return GzipJsonLinesWriter(destination, serializer=serializer)
        return JsonLinesFileWriter(destination, serializer=serializer)
    if format_ == "csv":
        serializer = (
            record_csv_row_serializer()
            if item_type == "record"
            else csv_row_serializer(payload)
        )
        return CsvFileWriter(destination, serializer=serializer)
    if format_ == "pickle":
        serializer = (
            record_pickle_serializer()
            if item_type == "record"
            else pickle_serializer(payload)
        )
        return PickleFileWriter(destination, serializer=serializer)

    raise ValueError(f"Unsupported fs format '{target.format}'")
