from datapipeline.io.factory import writer_factory
from datapipeline.io.output import OutputTarget
from datapipeline.io.writers import LineWriter
from datapipeline.io.sinks import StdoutTextSink


def _stdout_target(format_: str = "jsonl") -> OutputTarget:
    return OutputTarget(
        transport="stdout",
        format=format_,
        view="raw",
        encoding=None,
        destination=None,
    )


def test_writer_factory_stdout_jsonl_uses_plain_stdout_sink_even_with_visuals_on() -> None:
    writer = writer_factory(_stdout_target("jsonl"), visuals="on")
    assert isinstance(writer, LineWriter)
    assert isinstance(writer.sink, StdoutTextSink)


def test_writer_factory_stdout_jsonl_uses_plain_stdout_sink_with_visuals_off() -> None:
    writer = writer_factory(_stdout_target("jsonl"), visuals="off")
    assert isinstance(writer, LineWriter)
    assert isinstance(writer.sink, StdoutTextSink)
