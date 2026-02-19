from contextlib import contextmanager
from typing import Iterator, Any, Optional, Deque, Dict, Tuple
from math import ceil
import logging

from collections import deque

from rich.live import Live
from rich.progress import (
    Progress,
    ProgressColumn,
    TextColumn,
    BarColumn,
    MofNCompleteColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    Task,
)
from rich.text import Text

from .execution_context import current_dag_indent
from .source_observability import SourceObservabilityAdapter
from datapipeline.runtime import Runtime
from datapipeline.sources.models.source import Source
logger = logging.getLogger(__name__)


class AverageTimeRemainingColumn(ProgressColumn):
    """ETA column that blends long-term and recent throughput for stability."""

    max_refresh = 0.5

    def __init__(
        self,
        compact: bool = False,
        elapsed_when_finished: bool = False,
        table_column: Optional[Any] = None,
        window_seconds: float = 300.0,
    ) -> None:
        self.compact = compact
        self.elapsed_when_finished = elapsed_when_finished
        self.window_seconds = max(0.0, float(window_seconds))
        self._history: Dict[int, Deque[Tuple[float, float]]] = {}
        super().__init__(table_column=table_column)

    def _format_seconds(self, seconds: int) -> str:
        minutes, secs = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if self.compact and not hours:
            return f"{minutes:02d}:{secs:02d}"
        return f"{hours:d}:{minutes:02d}:{secs:02d}"

    def _recent_seconds_per_item(self, task: Task) -> Optional[float]:
        if self.window_seconds <= 0:
            return None
        if task.start_time is None:
            return None
        history = self._history.setdefault(int(task.id), deque())
        now = task.get_time()
        completed = float(task.completed)
        if not history or history[-1][1] != completed:
            history.append((now, completed))
        cutoff = now - self.window_seconds
        while history and history[0][0] < cutoff:
            history.popleft()
        if len(history) < 2:
            return None
        start_time, start_completed = history[0]
        delta_completed = completed - start_completed
        delta_time = now - start_time
        if delta_completed <= 0 or delta_time <= 0:
            return None
        return delta_time / delta_completed

    def render(self, task: Task) -> Text:
        if self.elapsed_when_finished and task.finished:
            self._history.pop(int(task.id), None)
            elapsed = task.finished_time
            if elapsed is None:
                return Text("-:--:--", style="progress.elapsed")
            return Text(self._format_seconds(int(elapsed)), style="progress.elapsed")

        style = "progress.remaining"
        total = task.total
        if total is None:
            return Text("", style=style)
        elapsed = task.elapsed
        completed = task.completed
        remaining = task.remaining
        if not completed or elapsed is None or remaining is None:
            return Text("--:--" if self.compact else "-:--:--", style=style)
        recent = self._recent_seconds_per_item(task)
        avg_seconds_per_item = recent if recent is not None else (elapsed / completed)
        if avg_seconds_per_item <= 0:
            return Text("--:--" if self.compact else "-:--:--", style=style)
        eta_seconds = int(max(0, ceil(remaining * avg_seconds_per_item)))
        return Text(self._format_seconds(eta_seconds), style=style)


class _RichSourceProxy(Source):
    def __init__(self, stream_source: Source, stream_id: str, progress: Progress, unit: Optional[str] = None, finalize: Optional[callable] = None, started: Optional[callable] = None):
        self._inner = stream_source
        self._stream_id = stream_id
        self._progress = progress
        self._task_id = None
        self._unit = unit
        self._emitted = 0
        self._finalize = finalize
        self._started = started

    def _format_text(self, message: str) -> str:
        # Plain stream-id prefix to avoid Rich markup issues
        indent = current_dag_indent()
        return f"{indent}[{self._stream_id}] {message}" if message else f"{indent}[{self._stream_id}]"

    def stream(self) -> Iterator[Any]:
        adapter = SourceObservabilityAdapter(self._inner, self._stream_id)
        self._unit = adapter.unit

        self._task_id = self._progress.add_task(
            "",
            start=False,
            total=None,
            text=self._format_text(adapter.format_label(include_stream_id=False, include_dag_indent=False)),
        )

        if self._task_id is not None:
            total = adapter.count()
            if total is not None:
                self._progress.update(self._task_id, total=total)

        emitted = 0
        last_path_label: Optional[str] = None
        started_logged = False

        if self._task_id is not None:
            self._progress.start_task(self._task_id)

        try:
            for item in self._inner.stream():
                current_label = adapter.current_label()
                # On first item: emit Start + transport details
                if not started_logged:
                    adapter.log_composed_details()
                    try:
                        if callable(self._started):
                            info_lines = adapter.info_lines()
                            debug_lines = adapter.debug_lines()
                            self._started(self._stream_id, info_lines, debug_lines)
                    except Exception:
                        pass
                    started_logged = True
                if current_label and current_label != last_path_label:
                    last_path_label = current_label
                    text = self._format_text(adapter.format_label(current_label, include_stream_id=False, include_dag_indent=False))
                    if self._task_id is not None:
                        self._progress.update(self._task_id, text=text)
                if self._task_id is not None:
                    self._progress.advance(self._task_id, 1)
                emitted += 1
                yield item
        finally:
            try:
                if self._task_id is not None:
                    self._progress.update(self._task_id, completed=emitted)
                    self._progress.stop_task(self._task_id)
                unit = self._unit or "item"
                unit_suffix = "" if emitted == 1 else "s"
                completed_text = f"{adapter.current_indent()}[{self._stream_id}] Stream complete ({emitted} {unit}{unit_suffix})"
                if callable(self._finalize):
                    try:
                        self._finalize(self._stream_id, completed_text)
                    except Exception:
                        pass
            except Exception:
                pass
            # Defer logging of completion to the session footer to avoid interleaving
            self._emitted = emitted
            # No explicit end separator; completion line is sufficient


@contextmanager
def visual_sources(runtime: Runtime, log_level: int | None):
    level = log_level if log_level is not None else logging.INFO
    if level > logging.INFO:
        yield
        return

    # Build a console on stderr for visuals/logs
    from rich.console import Console as _Console
    import sys as _sys
    _vis_console = _Console(file=_sys.stderr, markup=False,
                            highlight=False, soft_wrap=True)

    columns = [
        TextColumn("{task.fields[text]}", markup=False),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        AverageTimeRemainingColumn(),
    ]

    # Keep Live output transient so the spinner/bars disappear once completed
    progress = Progress(*columns, transient=True, console=_vis_console)

    # Install a temporary Rich logging handler for clean log rendering during Live
    class _DedupFilter(logging.Filter):
        def __init__(self):
            super().__init__()
            self._last: tuple[int, str] | None = None

        # type: ignore[override]
        def filter(self, record: logging.LogRecord) -> bool:
            try:
                msg = record.getMessage()
            except Exception:
                msg = record.msg if isinstance(
                    record.msg, str) else str(record.msg)
            key = (record.levelno, msg)
            if self._last == key:
                return False
            self._last = key
            return True

    rich_handler = None
    root_logger = logging.getLogger()
    old_handlers = list(root_logger.handlers)
    old_filters = list(root_logger.filters)
    try:
        from rich.logging import RichHandler
        console = _vis_console
        rich_handler = RichHandler(
            console=console,
            show_time=False,
            show_level=False,
            show_path=False,
            markup=False,
            rich_tracebacks=False,
        )
    except Exception:
        rich_handler = None

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())
    # Swap handlers if RichHandler is available
    if rich_handler is not None:
        # Replace handlers with Rich and add a simple de-dup filter to avoid
        # double-rendered lines if another handler slips in.
        root_logger.handlers = [rich_handler]
        dedup = _DedupFilter()
        root_logger.addFilter(dedup)

    renderable = progress

    with Live(renderable, console=_vis_console, refresh_per_second=10, transient=True):
        try:
            active_stream_id: Optional[str] = None
            pending_starts: list[tuple[str, list[tuple[str, str]]]] = []
            seen_messages: set[str] = set()

            def _emit_entries(entries: list[tuple[str, str]]) -> None:
                for level, line in entries:
                    key = f"{level}:{line}"
                    if key in seen_messages:
                        continue
                    seen_messages.add(key)
                    if level == "debug":
                        logger.debug(line)
                    else:
                        logger.info(line)

            def _flush_next_start() -> None:
                nonlocal active_stream_id
                if active_stream_id is not None:
                    return
                while pending_starts:
                    next_stream_id, entries = pending_starts.pop(0)
                    if not entries:
                        continue
                    active_stream_id = next_stream_id
                    _emit_entries(entries)
                    break

            def _append_completed(stream_id: str, text: str):
                _emit_entries([("info", f"{text} ✔")])
                nonlocal active_stream_id
                if active_stream_id == stream_id:
                    active_stream_id = None
                _flush_next_start()

            def _append_started(stream_id: str, info_lines: list[str], debug_lines: list[str]):
                nonlocal active_stream_id
                entries: list[tuple[str, str]] = []
                indent = current_dag_indent()
                for line in info_lines:
                    entries.append(("info", f"{indent}[{stream_id}] {line}"))
                for line in debug_lines:
                    entries.append(("debug", f"{indent}[{stream_id}] {line}"))
                if not entries:
                    entries = [("info", f"{indent}[{stream_id}] Stream starting")]
                if active_stream_id is None:
                    active_stream_id = stream_id
                    _emit_entries(entries)
                    return
                pending_starts.append((stream_id, entries))
            for stream_id, stream_source in originals.items():
                proxy = _RichSourceProxy(
                    stream_source=stream_source,
                    stream_id=stream_id,
                    progress=progress,
                    finalize=_append_completed,
                    started=_append_started,
                )
                reg.register(stream_id, proxy)
            yield
        finally:
            # Restore original sources
            for stream_id, stream_source in originals.items():
                reg.register(stream_id, stream_source)
    # After Live finishes: restore logging handlers
    if rich_handler is not None:
        # Restore original handlers and filters
        root_logger.handlers = old_handlers
        root_logger.filters = old_filters
