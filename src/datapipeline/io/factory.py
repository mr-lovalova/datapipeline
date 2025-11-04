from datapipeline.io.writers import (
    JsonLinesFileWriter,
    JsonLinesStdoutWriter,
    GzipJsonLinesWriter,
    CsvFileWriter,
    PickleFileWriter,
    LineWriter,
)
from datapipeline.io.protocols import Writer
from datapipeline.io.formatters import PrintLineFormatter
from datapipeline.io.sinks import StdoutTextSink
from datapipeline.io.output import OutputTarget


def writer_factory(target: OutputTarget) -> Writer:
    transport = target.transport.lower()
    format_ = target.format.lower()

    if transport == "stdout":
        if format_ in {"json-lines", "json", "jsonl"}:
            return JsonLinesStdoutWriter()
        if format_ == "print":
            return LineWriter(StdoutTextSink(), PrintLineFormatter())
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
