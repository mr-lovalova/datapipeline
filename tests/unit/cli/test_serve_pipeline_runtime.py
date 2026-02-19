from dataclasses import dataclass, field

from datapipeline.cli.commands.serve_pipeline import (
    _should_emit_stdout_separator,
    serve_stream,
)
from datapipeline.io.output import OutputTarget


@dataclass
class _Writer:
    writes: list[object] = field(default_factory=list)
    closed: bool = False

    def write(self, item: object) -> None:
        self.writes.append(item)

    def close(self) -> None:
        self.closed = True


def _target(transport: str) -> OutputTarget:
    return OutputTarget(
        transport=transport,
        format="print",
        view="raw",
        encoding=None,
        destination=None,
    )


def test_serve_stream_emits_single_stderr_separator_when_enabled(capsys):
    writer = _Writer()
    count = serve_stream(
        iter([1, 2]),
        limit=None,
        writer=writer,
        emit_stdout_separator=True,
    )

    captured = capsys.readouterr()
    assert count == 2
    assert writer.writes == [1, 2]
    assert writer.closed is True
    assert captured.out == "\n"


def test_serve_stream_skips_stderr_separator_when_disabled(capsys):
    writer = _Writer()
    count = serve_stream(
        iter([1]),
        limit=None,
        writer=writer,
        emit_stdout_separator=False,
    )

    captured = capsys.readouterr()
    assert count == 1
    assert writer.writes == [1]
    assert writer.closed is True
    assert captured.out == ""


def test_should_emit_stdout_separator_depends_on_transport_and_visuals():
    stdout_target = _target("stdout")
    fs_target = _target("fs")

    assert _should_emit_stdout_separator(stdout_target, "on") is True
    assert _should_emit_stdout_separator(stdout_target, "auto") is True
    assert _should_emit_stdout_separator(stdout_target, "off") is False
    assert _should_emit_stdout_separator(stdout_target, None) is True
    assert _should_emit_stdout_separator(fs_target, "on") is False
