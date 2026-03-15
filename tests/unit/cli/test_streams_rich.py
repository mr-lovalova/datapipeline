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
    current_terminal_log_proxy_sink,
    current_visual_log_level,
    reset_current_execution_event_sink,
    reset_current_terminal_log_proxy_sink,
    reset_current_visual_log_level,
    set_current_dag_depth,
    set_current_execution_event_sink,
    set_current_terminal_log_proxy_sink,
    set_current_visual_log_level,
)
from datapipeline.cli.visuals.rich.columns import SourceLabelColumn
from datapipeline.cli.visuals.rich.event_sink import _RichConsoleExecutionSink
from datapipeline.cli.visuals.rich.sources import (
    _RichSourceProxy,
    _clear_progress_tasks,
    visual_sources,
)
from datapipeline.cli.visuals.streams import observe_source
from datapipeline.sources.models.generator import DataGenerator
from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.models.loader import BaseDataLoader, SyntheticLoader
from datapipeline.sources.adapters.fs import FsGlobTransport


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


class _RecordingProgress:
    def __init__(self) -> None:
        self._next_task_id = 1
        self.task_text: dict[int, str] = {}
        self.added_texts: list[str] = []
        self.updated_texts: list[str] = []
        self.updated_by_task: dict[int, list[str]] = {}
        self.removed: list[int] = []

    def add_task(self, *args, **kwargs):
        task_id = self._next_task_id
        self._next_task_id += 1
        text = str(kwargs.get("text", ""))
        self.task_text[int(task_id)] = text
        self.added_texts.append(text)
        return task_id

    def update(self, task_id, *args, **kwargs):
        if "text" in kwargs:
            text = str(kwargs["text"])
            self.task_text[int(task_id)] = text
            self.updated_texts.append(text)
            self.updated_by_task.setdefault(int(task_id), []).append(text)
        return None

    def start_task(self, *args, **kwargs):
        return None

    def advance(self, *args, **kwargs):
        return None

    def stop_task(self, *args, **kwargs):
        return None

    def remove_task(self, task_id):
        self.removed.append(int(task_id))
        self.task_text.pop(int(task_id), None)

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
            step_count=3,
        )
    )
    sink.emit(
        ExecutionLogEvent(
            kind="dag_end",
            dag_name="pipeline:serve",
            depth=0,
            step_count=3,
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
            step_count=3,
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


def test_rich_execution_sink_styles_warning_messages() -> None:
    buffer = StringIO()
    console = Console(file=buffer, markup=False, highlight=False, force_terminal=False)
    sink = _RichConsoleExecutionSink(level=0, console=console)

    text = sink._render_event(
        ExecutionLogEvent(
            kind="message",
            dag_name="",
            depth=0,
            message="skipped tick(s) for partition '()'",
            log_level=logging.WARNING,
        )
    )

    assert str(text) == "skipped tick(s) for partition '()'"
    assert any(span.style == "yellow" for span in text.spans)


def test_rich_execution_sink_styles_error_messages() -> None:
    buffer = StringIO()
    console = Console(file=buffer, markup=False, highlight=False, force_terminal=False)
    sink = _RichConsoleExecutionSink(level=0, console=console)

    text = sink._render_event(
        ExecutionLogEvent(
            kind="message",
            dag_name="",
            depth=0,
            message="failed to materialize artifact",
            log_level=logging.ERROR,
        )
    )

    assert str(text) == "failed to materialize artifact"
    assert any(span.style == "bold red" for span in text.spans)


def test_rich_execution_sink_renders_source_info_message() -> None:
    buffer = StringIO()
    console = Console(file=buffer, markup=False, highlight=False, force_terminal=False)
    sink = _RichConsoleExecutionSink(level=0, console=console)
    sink.emit(
        ExecutionLogEvent(
            kind="message",
            dag_name="",
            depth=1,
            message='[equity.ohlcv] fs.glob: 2 files (first=APPL.jsonl, last=MSFT.jsonl)',
            message_kind="source_info",
            log_level=logging.INFO,
        )
    )

    lines = _lines(buffer)
    assert any(
        line.startswith('  [equity.ohlcv] fs.glob: 2 files (first=APPL.jsonl, last=MSFT.jsonl)')
        for line in lines
    )


def test_rich_execution_sink_renders_scope_start_as_header() -> None:
    buffer = StringIO()
    console = Console(file=buffer, markup=False, highlight=False, force_terminal=False)
    sink = _RichConsoleExecutionSink(level=0, console=console)
    sink.emit(
        ExecutionLogEvent(
            kind="message",
            dag_name="",
            depth=0,
            message="Scope: profile=serve:test target=serve task=schema",
            message_kind="scope_start",
            log_level=logging.INFO,
            scope_profile_kind="serve",
            scope_profile_name="test",
            scope_target_id="serve",
            scope_task_id="schema",
            scope_item_index="1",
            scope_item_total="3",
        )
    )

    lines = _lines(buffer)
    assert any("Task: schema (1/3)" in line for line in lines)


def test_rich_execution_sink_keeps_fs_glob_source_info_when_live_at_info() -> None:
    buffer = StringIO()
    console = Console(file=buffer, markup=False, highlight=False, force_terminal=False)
    sink = _RichConsoleExecutionSink(level=logging.INFO, console=console)
    sink.set_live_console(console)
    sink.emit(
        ExecutionLogEvent(
            kind="message",
            dag_name="",
            depth=1,
            message='[equity.ohlcv] fs.glob: 2 files (first=APPL.jsonl, last=MSFT.jsonl)',
            message_kind="source_info",
            log_level=logging.INFO,
        )
    )
    assert any(
        line.startswith('  [equity.ohlcv] fs.glob: 2 files (first=APPL.jsonl, last=MSFT.jsonl)')
        for line in _lines(buffer)
    )


def test_rich_execution_sink_keeps_non_glob_source_info_when_live_at_info() -> None:
    buffer = StringIO()
    console = Console(file=buffer, markup=False, highlight=False, force_terminal=False)
    sink = _RichConsoleExecutionSink(level=logging.INFO, console=console)
    sink.set_live_console(console)
    sink.emit(
        ExecutionLogEvent(
            kind="message",
            dag_name="",
            depth=1,
            message='[time.ticks.linear] synthetic.generate: start=2021-01-01T01:00:00Z end=2021-02-01T01:00:00Z freq=1d',
            message_kind="source_info",
            log_level=logging.INFO,
        )
    )
    assert any(
        line.startswith(
            "  [time.ticks.linear] synthetic.generate: start=2021-01-01T01:00:00Z"
        )
        for line in _lines(buffer)
    )


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


def test_source_label_column_does_not_infer_indent_from_context() -> None:
    set_current_dag_depth(3)
    try:
        text = SourceLabelColumn().render(SimpleNamespace(fields={"text": "[equity.ohlcv] Loading"}))
        assert str(text).startswith("[equity.ohlcv] Loading")
    finally:
        set_current_dag_depth(0)


def test_rich_source_proxy_formats_text_with_context_indent() -> None:
    proxy = _RichSourceProxy(
        stream_source=_SyntheticSource(),
        stream_id="equity.ohlcv",
        progress=SimpleNamespace(),
    )
    set_current_dag_depth(3)
    try:
        text = proxy._format_text("equity.ohlcv", 'Loading "MSFT.jsonl"')
        assert text.startswith('      [equity.ohlcv] Loading "MSFT.jsonl"')
    finally:
        set_current_dag_depth(0)


def test_rich_source_proxy_emits_glob_summary_as_source_info_event(monkeypatch) -> None:
    captured: list[tuple[str, int, int, str | None]] = []
    monkeypatch.setattr(
        "datapipeline.cli.visuals.rich.sources.emit_execution_message",
        lambda message, level, logger, depth=0, message_kind=None: captured.append(
            (message, level, depth, message_kind)
        ),
    )

    class _Adapter:
        def __init__(self):
            transport = FsGlobTransport("/definitely/not/real/*.jsonl")
            transport._files = ["/tmp/APPL.jsonl", "/tmp/MSFT.jsonl"]  # type: ignore[attr-defined]
            self.loader = SimpleNamespace(transport=transport)

        def info_lines(self):
            return ["fs.glob: 2 files (first=APPL.jsonl, last=MSFT.jsonl)"]

        def format_label(self, name=None, **kwargs):
            if name:
                return f"Loading {name}"
            return "Loading"

        def initial_label(self):
            return None

        def count(self):
            return None

        def current_label(self):
            return None

    monkeypatch.setattr(
        "datapipeline.cli.visuals.rich.sources.SourceObservabilityAdapter",
        lambda stream_source, stream_id: _Adapter(),
    )

    progress = _RecordingProgress()
    source = SimpleNamespace(stream=lambda: iter([1]))
    proxy = _RichSourceProxy(
        stream_source=source,
        stream_id="equity.ohlcv",
        progress=progress,
    )

    set_current_dag_depth(2)
    try:
        list(proxy.stream())
    finally:
        set_current_dag_depth(0)

    assert captured
    assert captured[0] == (
        "[equity.ohlcv] fs.glob: 2 files (first=APPL.jsonl, last=MSFT.jsonl)",
        logging.INFO,
        2,
        "source_info",
    )
    assert any(
        msg == "[equity.ohlcv] Stream complete items=1"
        and kind == "source_info"
        for msg, _level, _depth, kind in captured
    )


def test_rich_source_proxy_tracks_glob_file_transitions_as_progress_rows(monkeypatch) -> None:
    class _Adapter:
        def __init__(self):
            transport = FsGlobTransport("/definitely/not/real/*.jsonl")
            transport._files = ["/tmp/APPL.jsonl", "/tmp/MSFT.jsonl"]  # type: ignore[attr-defined]
            self.loader = SimpleNamespace(transport=transport)

        def info_lines(self):
            return ["fs.glob: 2 files (first=APPL.jsonl, last=MSFT.jsonl)"]

        def format_label(self, name=None, **kwargs):
            if name:
                return f"Loading {name}"
            return "Loading"

        def initial_label(self):
            return '"APPL.jsonl"'

        def count(self):
            return None

        def current_label(self):
            return '"MSFT.jsonl"'

    monkeypatch.setattr(
        "datapipeline.cli.visuals.rich.sources.SourceObservabilityAdapter",
        lambda stream_source, stream_id: _Adapter(),
    )

    progress = _RecordingProgress()
    source = SimpleNamespace(stream=lambda: iter([1]))
    proxy = _RichSourceProxy(
        stream_source=source,
        stream_id="equity.ohlcv",
        progress=progress,
    )

    set_current_dag_depth(2)
    try:
        list(proxy.stream())
    finally:
        set_current_dag_depth(0)

    assert any('Loading "APPL.jsonl"' in text for text in progress.added_texts)
    assert any('Loading "MSFT.jsonl"' in text for text in progress.updated_texts)


def test_rich_source_proxy_clears_glob_rows_between_invocations(monkeypatch) -> None:
    class _Adapter:
        def __init__(self):
            transport = FsGlobTransport("/definitely/not/real/*.jsonl")
            transport._files = ["/tmp/APPL.jsonl", "/tmp/MSFT.jsonl"]  # type: ignore[attr-defined]
            self.loader = SimpleNamespace(transport=transport)

        def info_lines(self):
            return ["fs.glob: 2 files (first=APPL.jsonl, last=MSFT.jsonl)"]

        def format_label(self, name=None, **kwargs):
            if name:
                return f"Loading {name}"
            return "Loading"

        def initial_label(self):
            return '"APPL.jsonl"'

        def count(self):
            return None

        def current_label(self):
            return '"MSFT.jsonl"'

    monkeypatch.setattr(
        "datapipeline.cli.visuals.rich.sources.SourceObservabilityAdapter",
        lambda stream_source, stream_id: _Adapter(),
    )

    progress = _RecordingProgress()
    source = SimpleNamespace(stream=lambda: iter([1]))
    proxy = _RichSourceProxy(
        stream_source=source,
        stream_id="equity.ohlcv",
        progress=progress,
    )

    set_current_dag_depth(2)
    try:
        list(proxy.stream())
        assert progress.task_text == {}
        first_run_count = len(progress.added_texts)
        list(proxy.stream())
        assert progress.task_text == {}
        assert len(progress.added_texts) == first_run_count * 2
    finally:
        set_current_dag_depth(0)


def test_rich_source_proxy_handles_foreach_fs_sequence_with_empty_first_value(monkeypatch) -> None:
    captured: list[tuple[str, int, int, str | None]] = []
    monkeypatch.setattr(
        "datapipeline.cli.visuals.rich.sources.emit_execution_message",
        lambda message, level, logger, depth=0, message_kind=None: captured.append(
            (message, level, depth, message_kind)
        ),
    )

    class _ForeachValueLoader(BaseDataLoader):
        def __init__(self, path: str | None = None, value: str | None = None, **kwargs):
            self._path = str(path or value or "")
            self._rows = 0 if "APPL" in self._path else 1

        def load(self):
            for idx in range(self._rows):
                yield {"path": self._path, "idx": idx}

    monkeypatch.setattr(
        "datapipeline.sources.foreach.load_ep",
        lambda _namespace, _entrypoint: _ForeachValueLoader,
    )

    foreach_loader = ForeachLoader(
        foreach={"path": ["/tmp/APPL.jsonl", "/tmp/MSFT.jsonl"]},
        loader={
            "entrypoint": "core.io",
            "args": {
                "transport": "fs",
                "path": "${path}",
            },
        },
    )
    source = SimpleNamespace(stream=lambda: foreach_loader.load(), loader=foreach_loader)
    progress = _RecordingProgress()
    proxy = _RichSourceProxy(
        stream_source=source,
        stream_id="equity.ohlcv",
        progress=progress,
    )

    set_current_dag_depth(2)
    try:
        list(proxy.stream())
    finally:
        set_current_dag_depth(0)

    assert captured
    assert any(
        msg == "[equity.ohlcv] fs.glob: 2 files (first=APPL.jsonl, last=MSFT.jsonl)"
        and kind == "source_info"
        for msg, _level, _depth, kind in captured
    )
    assert any('Loading "APPL.jsonl"' in text for text in progress.added_texts)
    assert any('Loading "MSFT.jsonl"' in text for text in progress.added_texts)


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
        with caplog.at_level(logging.DEBUG, logger="datapipeline.cli.visuals.rich.sources"):
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

    assert progress.removed == []


def test_rich_source_proxy_keeps_task_updates_isolated_across_concurrent_streams(
    monkeypatch,
) -> None:
    class _Adapter:
        _instance = 0

        def __init__(self):
            self._id = _Adapter._instance
            _Adapter._instance += 1
            self._calls = 0

        def info_lines(self):
            return []

        def format_label(self, name=None, **kwargs):
            if name:
                return f"Loading {name}"
            return "Loading"

        def initial_label(self):
            return f'"APPL-{self._id}.jsonl"'

        def count(self):
            return 2

        def current_label(self):
            self._calls += 1
            if self._calls >= 2:
                return f'"MSFT-{self._id}.jsonl"'
            return f'"APPL-{self._id}.jsonl"'

    monkeypatch.setattr(
        "datapipeline.cli.visuals.rich.sources.SourceObservabilityAdapter",
        lambda stream_source, stream_id: _Adapter(),
    )

    source = SimpleNamespace(stream=lambda: iter([1, 2]))
    progress = _RecordingProgress()
    proxy = _RichSourceProxy(
        stream_source=source,
        stream_id="equity.ohlcv",
        progress=progress,
    )

    set_current_dag_depth(2)
    try:
        stream_one = proxy.stream()
        stream_two = proxy.stream()
        next(stream_one)
        next(stream_two)
        next(stream_one)
        next(stream_two)
        with pytest.raises(StopIteration):
            next(stream_one)
        with pytest.raises(StopIteration):
            next(stream_two)
    finally:
        set_current_dag_depth(0)

    assert progress.removed == [1, 2]
    assert any('Loading "MSFT-0.jsonl"' in text for text in progress.updated_by_task[1])
    assert any('Loading "MSFT-1.jsonl"' in text for text in progress.updated_by_task[2])


def test_visual_sources_resets_context_when_live_fails(monkeypatch) -> None:
    monkeypatch.setattr("datapipeline.cli.visuals.rich.sources.Progress", _BrokenProgress)
    runtime = SimpleNamespace(
        registries=SimpleNamespace(stream_sources=_StreamRegistry())
    )
    level_token = set_current_visual_log_level(logging.WARNING)
    sink_token = set_current_execution_event_sink("sentinel")
    proxy_token = set_current_terminal_log_proxy_sink("proxy-sentinel")
    set_current_dag_depth(3)
    try:
        with pytest.raises(RuntimeError, match="live boom"):
            with visual_sources(runtime, logging.INFO):
                pass

        assert current_visual_log_level() == logging.WARNING
        assert current_execution_event_sink() == "sentinel"
        assert current_terminal_log_proxy_sink() == "proxy-sentinel"
        assert current_dag_depth() == 0
    finally:
        reset_current_terminal_log_proxy_sink(proxy_token)
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
        "datapipeline.cli.visuals.rich.sources._clear_progress_tasks",
        _capture_clear,
    )

    with pytest.raises(KeyboardInterrupt):
        with visual_sources(runtime, logging.INFO):
            wrapped = runtime.registries.stream_sources._items["time.ticks.linear"]
            iterator = wrapped.stream()
            next(iterator)
            raise KeyboardInterrupt()

    assert called["count"] == 1


def test_rich_visual_sources_observe_dynamic_sources() -> None:
    runtime = SimpleNamespace(
        registries=SimpleNamespace(stream_sources=_StreamRegistry())
    )

    with visual_sources(runtime, logging.INFO):
        observed = observe_source(_SyntheticSource(), "time.ticks.linear")

    assert isinstance(observed, _RichSourceProxy)
