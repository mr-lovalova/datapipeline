import json
import pickle
from dataclasses import dataclass

import pytest

from datapipeline.io.factory import writer_factory
from datapipeline.io.output import OutputTarget
from datapipeline.io.sinks.files import AtomicTextFileSink
from datapipeline.io.sinks.stdout import StdoutTextSink
from datapipeline.io.writers.base import LineWriter


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


def test_writer_factory_fs_jsonl_applies_flat_view(tmp_path) -> None:
    destination = tmp_path / "out.jsonl"
    writer = writer_factory(
        OutputTarget(
            transport="fs",
            format="jsonl",
            view="flat",
            encoding="utf-8",
            destination=destination,
        )
    )

    writer.write({"features": {"x": 1.0}})
    writer.close()

    assert json.loads(destination.read_text(encoding="utf-8")) == {"features.x": 1.0}


def test_writer_factory_fs_pickle_writes_raw_payload(tmp_path) -> None:
    @dataclass
    class Payload:
        value: int

    destination = tmp_path / "out.pkl"
    writer = writer_factory(
        OutputTarget(
            transport="fs",
            format="pickle",
            view="raw",
            encoding=None,
            destination=destination,
        )
    )

    writer.write(Payload(1))
    writer.write(Payload(2))
    writer.close()

    with destination.open("rb") as fh:
        assert pickle.load(fh) == {"value": 1}
        assert pickle.load(fh) == {"value": 2}


@pytest.mark.parametrize(
    ("format_", "view", "message"),
    [
        ("csv", "raw", "csv output supports only view='flat'"),
        ("pickle", "flat", "pickle output supports only view='raw'"),
    ],
)
def test_writer_factory_rejects_unsupported_view(
    tmp_path,
    format_: str,
    view: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        writer_factory(
            OutputTarget(
                transport="fs",
                format=format_,
                view=view,
                encoding=None,
                destination=tmp_path / f"out.{format_}",
            )
        )
