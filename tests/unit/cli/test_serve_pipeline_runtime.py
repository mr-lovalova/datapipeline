from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest
from datapipeline.cli.commands.serve_pipeline import serve_stream
from datapipeline.cli.commands.serve_pipeline import serve_with_runtime


@dataclass
class _Writer:
    writes: list[object] = field(default_factory=list)
    closed: bool = False

    def write(self, item: object) -> None:
        self.writes.append(item)

    def close(self) -> None:
        self.closed = True


def test_serve_stream_writes_all_items_and_closes_writer(capsys):
    writer = _Writer()
    count = serve_stream(
        iter([1, 2]),
        limit=None,
        writer=writer,
    )

    captured = capsys.readouterr()
    assert count == 2
    assert writer.writes == [1, 2]
    assert writer.closed is True
    assert captured.out == ""


def test_serve_stream_honors_limit_and_closes_writer(capsys):
    writer = _Writer()
    count = serve_stream(
        iter([1, 2, 3]),
        limit=1,
        writer=writer,
    )

    captured = capsys.readouterr()
    assert count == 1
    assert writer.writes == [1]
    assert writer.closed is True
    assert captured.out == ""


def test_serve_with_runtime_reraises_keyboard_interrupt_and_marks_run_failed(monkeypatch):
    runtime = SimpleNamespace(window_bounds=None, execution_observer=None)
    dataset = SimpleNamespace(features=[object()], targets=[], group_by="1d")
    target = SimpleNamespace(run="run-paths", destination=None, transport="stdout")

    calls = {"failed": 0, "success": 0}

    monkeypatch.setattr(
        "datapipeline.cli.commands.serve_pipeline.resolve_window_bounds",
        lambda runtime_obj, rectangular_required: (None, None),
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.serve_pipeline.build_full_pipeline",
        lambda *args, **kwargs: iter(()),
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.serve_pipeline.writer_factory",
        lambda *args, **kwargs: object(),
    )

    def _raise_interrupt(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(
        "datapipeline.cli.commands.serve_pipeline.serve_stream",
        _raise_interrupt,
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.serve_pipeline.finish_run_failed",
        lambda _paths: calls.__setitem__("failed", calls["failed"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.serve_pipeline.finish_run_success",
        lambda _paths: calls.__setitem__("success", calls["success"] + 1),
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.serve_pipeline.set_latest_run",
        lambda _paths: None,
    )

    with pytest.raises(KeyboardInterrupt):
        serve_with_runtime(
            runtime=runtime,
            dataset=dataset,
            limit=None,
            target=target,
            throttle_ms=None,
            stage=None,
            visuals="on",
        )

    assert calls["failed"] == 1
    assert calls["success"] == 0
