from datapipeline.io.protocols import Writer
from datapipeline.io.output import OutputTarget
from datapipeline.io.serializers import (
    csv_row_serializer,
    json_line_serializer,
    pickle_serializer,
    text_line_serializer,
)
from datapipeline.io.sinks import AtomicTextFileSink, StdoutTextSink
from datapipeline.io.writers import (
    CsvFileWriter,
    JsonLinesFileWriter,
    LineWriter,
    PickleFileWriter,
)


def writer_factory(target: OutputTarget) -> Writer:
    if target.transport == "stdout":
        if target.format == "jsonl":
            return LineWriter(StdoutTextSink(), json_line_serializer(target.view))
        if target.format == "txt":
            return LineWriter(StdoutTextSink(), text_line_serializer())
        raise ValueError(f"Unsupported stdout format '{target.format}'")

    destination = target.destination
    if destination is None:
        raise ValueError("fs output requires a destination path")
    text_encoding = target.encoding or "utf-8"

    if target.format == "jsonl":
        return JsonLinesFileWriter(
            destination,
            serializer=json_line_serializer(target.view),
            encoding=text_encoding,
        )
    if target.format == "csv":
        return CsvFileWriter(
            destination,
            serializer=csv_row_serializer(target.view),
            encoding=text_encoding,
        )
    if target.format == "pickle":
        return PickleFileWriter(
            destination,
            serializer=pickle_serializer(target.view),
        )
    if target.format == "txt":
        return LineWriter(
            AtomicTextFileSink(
                destination,
                encoding=text_encoding,
            ),
            text_line_serializer(),
        )

    raise ValueError(f"Unsupported fs format '{target.format}'")
