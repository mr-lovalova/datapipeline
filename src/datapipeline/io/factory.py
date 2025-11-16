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
from datapipeline.io.formatters import PrintLineFormatter, JsonLineFormatter
from datapipeline.io.sinks import StdoutTextSink, RichStdoutSink, ReprRichFormatter, JsonRichFormatter, PlainRichFormatter
from datapipeline.io.output import OutputTarget


def stdout_sink_for(format_: str, visuals: Optional[str]) -> StdoutTextSink:
    fmt = (format_ or "print").lower()
    provider = (visuals or "auto").lower()
    if provider != "rich":
        return StdoutTextSink()
    try:
        if fmt in {"json", "json-lines", "jsonl"}:
            return RichStdoutSink(JsonRichFormatter())
        if fmt == "print":
            return RichStdoutSink(ReprRichFormatter())
        return RichStdoutSink(PlainRichFormatter())
    except Exception:
        return StdoutTextSink()


def writer_factory(target: OutputTarget, *, visuals: Optional[str] = None) -> Writer:
    transport = target.transport.lower()
    format_ = target.format.lower()

    if transport == "stdout":
        sink = stdout_sink_for(format_, visuals)
        if format_ in {"json-lines", "json", "jsonl"}:
            return LineWriter(sink, JsonLineFormatter())
        if format_ == "print":
            return LineWriter(sink, PrintLineFormatter())
        raise ValueError(f"Unsupported stdout format '{target.format}'")

    destination = target.destination
    if destination is None:
        raise ValueError("fs output requires a destination path")
    destination.parent.mkdir(parents=True, exist_ok=True)

    suffix = "".join(destination.suffixes).lower()
    if format_ in {"json-lines", "json", "jsonl"}:
        if suffix.endswith(".jsonl.gz") or suffix.endswith(".json.gz") or suffix.endswith(".gz"):
            return GzipJsonLinesWriter(destination)
        return JsonLinesFileWriter(destination)
    if format_ == "csv":
        return CsvFileWriter(destination)
    if format_ == "pickle":
        return PickleFileWriter(destination)

    raise ValueError(f"Unsupported fs format '{target.format}'")
