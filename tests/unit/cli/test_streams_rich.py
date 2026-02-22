from io import StringIO
import logging
from types import SimpleNamespace

import pytest
from rich.console import Console
from rich.progress import Progress

from datapipeline.cli.visuals.execution import ExecutionLogEvent
from datapipeline.cli.visuals.execution_context import (
    current_dag_depth,
    current_execution_event_sink,
    current_visual_log_level,
    reset_current_execution_event_sink,
    reset_current_visual_log_level,
    set_current_dag_depth,
    set_current_execution_event_sink,
    set_current_visual_log_level,
)
from datapipeline.cli.visuals.streams_rich import (
    _RichConsoleExecutionSink,
    _RichSourceProxy,
    _clear_progress_tasks,
    SourceLabelColumn,
    visual_sources,
)
from datapipeline.sources.models.generator import DataGenerator
from datapipeline.sources.models.loader import SyntheticLoader


class _DummyGenerator(DataGenerator):
    def generate(self):
        for i in range(3):
            yield {"n": i}

    def count(self):
        return 3

    def info_lines(self):
        return ["synthetic.generate: start=2024-01-01T00:00:00Z end=2024-01-01T02:00:00Z freq=1h"]


class _SyntheticSource:
    def __init__(self) -> None:
        self.loader = SyntheticLoader(_DummyGenerator())

    def stream(self):
        yield from self.loader.load()


class _InterruptCountGenerator(DataGenerator):
    def generate(self):
        yield {"n": 0}

    def count(self):
        raise KeyboardInterrupt()

    def info_lines(self):
        return ["synthetic.generate: interrupt-on-count"]


class _InterruptCountSource:
    def __init__(self) -> None:
        self.loader = SyntheticLoader(_InterruptCountGenerator())

    def stream(self):
        yield from self.loader.load()


class _StreamRegistry:
    def __init__(self) -> None:
        self._items: dict[str, object] = {}

    def items(self):
        return self._items.items()

    def register(self, stream_id: str, stream_source: object) -> None:
        self._items[stream_id] = stream_source


class _BrokenProgress:
    def __init__(self, *args, **kwargs) -> None:
        self.live = None

    def __enter__(self):
        raise RuntimeError("live boom")

    def __exit__(self, exc_type, exc, tb):
        return False

    def add_task(self, *args, **kwargs):
        return 0

    def update(self, *args, **kwargs):
        return None

    def start_task(self, *args, **kwargs):
        return None

    def advance(self, *args, **kwargs):
        return None

    def stop_task(self, *args, **kwargs):
        return None

    def remove_task(self, *args, **kwargs):
        return None

    def refresh(self):
        return None


class _InterruptingAddProgress:
    def __init__(self) -> None:
        self._calls = 0
        self.removed: list[int] = []

    def add_task(self, *args, **kwargs):
        self._calls += 1
        if self._calls == 1:
            raise KeyboardInterrupt()
        return 101

    def update(self, *args, **kwargs):
        return None

    def start_task(self, *args, **kwargs):
        return None

    def advance(self, *args, **kwargs):
        return None

    def stop_task(self, *args, **kwargs):
        return None

    def remove_task(self, task_id):
        self.removed.append(int(task_id))

    def refresh(self):
        return None


def _lines(buffer: StringIO) -> list[str]:
    return [line for line in buffer.getvalue().splitlines() if line.strip()]


def test_rich_execution_sink_emits_dag_end_immediately() -> None:
    buffer = StringIO()
    console = Console(file=buffer, markup=False, highlight=False, force_terminal=False)
    sink = _RichConsoleExecutionSink(level=0, console=console)
    sink.set_live_console(console)

    sink.emit(
        ExecutionLogEvent(
            kind="dag_start",
            dag_name="pipeline:serve",
            depth=0,
            node_count=3,
        )
    )
    sink.emit(
        ExecutionLogEvent(
            kind="dag_end",
            dag_name="pipeline:serve",
            depth=0,
            node_count=3,
            status="success",
            output_items=1,
            elapsed_seconds=0.5,
        )
    )

    current = _lines(buffer)
    assert any(line.startswith("DAG started name=pipeline:serve") for line in current)
    assert any(line.startswith("DAG finished name=pipeline:serve") for line in current)


def test_rich_execution_sink_renders_error_type_for_failed_dag_end() -> None:
    buffer = StringIO()
    console = Console(file=buffer, markup=False, highlight=False, force_terminal=False)
    sink = _RichConsoleExecutionSink(level=0, console=console)
    sink.emit(
        ExecutionLogEvent(
            kind="dag_end",
            dag_name="pipeline:serve",
            depth=0,
            node_count=3,
            status="error",
            error_type="KeyboardInterrupt",
            output_items=0,
            elapsed_seconds=0.5,
        )
    )

    lines = _lines(buffer)
    assert any(
        line.startswith(
            "DAG finished name=pipeline:serve status=error error=KeyboardInterrupt"
        )
        for line in lines
    )


def test_rich_execution_sink_renders_message_event() -> None:
    buffer = StringIO()
    console = Console(file=buffer, markup=False, highlight=False, force_terminal=False)
    sink = _RichConsoleExecutionSink(level=0, console=console)
    sink.emit(
        ExecutionLogEvent(
            kind="message",
            dag_name="",
            depth=0,
            message="Saved 14 items: /tmp/train.jsonl",
            message_kind="saved",
            log_level=logging.INFO,
        )
    )

    lines = _lines(buffer)
    assert any(line.startswith("Saved 14 items: /tmp/train.jsonl") for line in lines)


def test_rich_execution_sink_indents_multiline_message_event_by_depth() -> None:
    buffer = StringIO()
    console = Console(file=buffer, markup=False, highlight=False, force_terminal=False)
    sink = _RichConsoleExecutionSink(level=0, console=console)
    sink.emit(
        ExecutionLogEvent(
            kind="message",
            dag_name="",
            depth=2,
            message="line1\nline2",
            log_level=logging.DEBUG,
        )
    )

    lines = _lines(buffer)
    assert lines[0].startswith("    line1")
    assert lines[1].startswith("    line2")


def test_source_label_column_is_single_line_truncated() -> None:
    column = SourceLabelColumn().get_table_column()
    assert column.no_wrap is True


def test_source_label_column_applies_visible_info_indent() -> None:
    set_current_dag_depth(3)
    try:
        text = SourceLabelColumn().render(SimpleNamespace(fields={"text": "[equity.ohlcv] Loading"}))
        assert str(text).startswith("  [equity.ohlcv] Loading")
    finally:
        set_current_dag_depth(0)


def test_rich_source_proxy_removes_finished_stream_task() -> None:
    console = Console(
        file=StringIO(),
        markup=False,
        highlight=False,
        force_terminal=False,
    )
    progress = Progress(SourceLabelColumn(), console=console)
    proxy = _RichSourceProxy(
        stream_source=_SyntheticSource(),
        stream_id="time.ticks.linear",
        progress=progress,
    )

    with progress:
        list(proxy.stream())

    assert proxy._task_id is None
    assert progress.tasks == []


def test_rich_source_proxy_cleans_tasks_when_count_is_interrupted() -> None:
    console = Console(
        file=StringIO(),
        markup=False,
        highlight=False,
        force_terminal=False,
    )
    progress = Progress(SourceLabelColumn(), console=console)
    proxy = _RichSourceProxy(
        stream_source=_InterruptCountSource(),
        stream_id="equity.ohlcv",
        progress=progress,
    )

    with progress:
        with pytest.raises(KeyboardInterrupt):
            list(proxy.stream())

    assert proxy._task_id is None
    assert progress.tasks == []


def test_rich_source_proxy_ignores_missing_task_during_teardown(caplog) -> None:
    console = Console(
        file=StringIO(),
        markup=False,
        highlight=False,
        force_terminal=False,
    )
    progress = Progress(SourceLabelColumn(), console=console)
    proxy = _RichSourceProxy(
        stream_source=_SyntheticSource(),
        stream_id="equity.ohlcv",
        progress=progress,
    )

    with progress:
        iterator = proxy.stream()
        next(iterator)
        _clear_progress_tasks(progress)
        with caplog.at_level(logging.DEBUG, logger="datapipeline.cli.visuals.streams_rich"):
            iterator.close()

    messages = [record.getMessage() for record in caplog.records]
    assert not any("visuals: failed to finalize progress task" in msg for msg in messages)
    assert not any("visuals: failed to stop progress task" in msg for msg in messages)
    assert not any("visuals: failed to remove progress task" in msg for msg in messages)


def test_clear_progress_tasks_removes_orphan_rows() -> None:
    console = Console(
        file=StringIO(),
        markup=False,
        highlight=False,
        force_terminal=False,
    )
    progress = Progress(SourceLabelColumn(), console=console)
    proxy = _RichSourceProxy(
        stream_source=_SyntheticSource(),
        stream_id="time.ticks.linear",
        progress=progress,
    )

    original_safe_call = proxy._safe_progress_call

    def _skip_remove(op, fn, *args, **kwargs):
        if op == "remove progress task":
            return None
        return original_safe_call(op, fn, *args, **kwargs)

    proxy._safe_progress_call = _skip_remove  # type: ignore[method-assign]

    with progress:
        list(proxy.stream())
        assert len(progress.tasks) == 1
        _clear_progress_tasks(progress)
        assert progress.tasks == []


def test_rich_source_proxy_cleans_meta_when_interrupted_during_task_setup() -> None:
    progress = _InterruptingAddProgress()
    proxy = _RichSourceProxy(
        stream_source=_SyntheticSource(),
        stream_id="equity.ohlcv",
        progress=progress,
    )

    with pytest.raises(KeyboardInterrupt):
        list(proxy.stream())

    assert proxy._task_id is None
    assert progress.removed == []


def test_visual_sources_resets_context_when_live_fails(monkeypatch) -> None:
    monkeypatch.setattr("datapipeline.cli.visuals.streams_rich.Progress", _BrokenProgress)
    runtime = SimpleNamespace(
        registries=SimpleNamespace(stream_sources=_StreamRegistry())
    )
    level_token = set_current_visual_log_level(logging.WARNING)
    sink_token = set_current_execution_event_sink("sentinel")
    set_current_dag_depth(3)
    try:
        with pytest.raises(RuntimeError, match="live boom"):
            with visual_sources(runtime, logging.INFO):
                pass

        assert current_visual_log_level() == logging.WARNING
        assert current_execution_event_sink() == "sentinel"
        assert current_dag_depth() == 0
    finally:
        reset_current_execution_event_sink(sink_token)
        reset_current_visual_log_level(level_token)


def test_visual_sources_runs_central_task_cleanup_on_interrupt(monkeypatch) -> None:
    runtime = SimpleNamespace(
        registries=SimpleNamespace(stream_sources=_StreamRegistry())
    )
    runtime.registries.stream_sources.register("time.ticks.linear", _SyntheticSource())

    called = {"count": 0}
    original_clear = _clear_progress_tasks

    def _capture_clear(progress):
        called["count"] += 1
        original_clear(progress)

    monkeypatch.setattr(
        "datapipeline.cli.visuals.streams_rich._clear_progress_tasks",
        _capture_clear,
    )

    with pytest.raises(KeyboardInterrupt):
        with visual_sources(runtime, logging.INFO):
            wrapped = runtime.registries.stream_sources._items["time.ticks.linear"]
            iterator = wrapped.stream()
            next(iterator)
            raise KeyboardInterrupt()

    assert called["count"] == 1
