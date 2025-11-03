from pathlib import Path

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
    kind = target.writer_kind
    destination = target.destination

    if destination is not None:
        destination.parent.mkdir(parents=True, exist_ok=True)

    match kind:
        case "stdout.jsonl":
            return JsonLinesStdoutWriter()
        case "stdout.print":
            return LineWriter(StdoutTextSink(), PrintLineFormatter())
        case "file.jsonl":
            assert destination is not None
            return JsonLinesFileWriter(destination)
        case "file.jsonl.gz":
            assert destination is not None
            return GzipJsonLinesWriter(destination)
        case "file.csv":
            assert destination is not None
            return CsvFileWriter(destination)
        case "file.pickle":
            assert destination is not None
            return PickleFileWriter(destination)
        case _:
            raise ValueError(f"Unknown writer kind: {kind}")
