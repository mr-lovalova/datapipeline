from datapipeline.io.factory import writer_factory
from datapipeline.io.output import OutputTarget
from datapipeline.io.writers import LineWriter
from datapipeline.io.sinks import AtomicTextFileSink, StdoutTextSink


def _stdout_target(format_: str = "jsonl") -> OutputTarget:
    return OutputTarget(
        transport="stdout",
        format=format_,
        view="raw",
        encoding=None,
        destination=None,
    )


def test_writer_factory_stdout_jsonl_uses_plain_stdout_sink() -> None:
    writer = writer_factory(_stdout_target("jsonl"))
    assert isinstance(writer, LineWriter)
    assert isinstance(writer.sink, StdoutTextSink)


def test_writer_factory_stdout_txt_uses_plain_stdout_sink() -> None:
    writer = writer_factory(_stdout_target("txt"))
    assert isinstance(writer, LineWriter)
    assert isinstance(writer.sink, StdoutTextSink)


def test_writer_factory_fs_txt_uses_line_writer(tmp_path) -> None:
    writer = writer_factory(
        OutputTarget(
            transport="fs",
            format="txt",
            view="flat",
            encoding="utf-8",
            destination=(tmp_path / "out.txt").resolve(),
        ),
    )
    assert isinstance(writer, LineWriter)
    assert isinstance(writer.sink, AtomicTextFileSink)
    writer.close()
