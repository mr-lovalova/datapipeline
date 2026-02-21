from contextlib import contextmanager
from typing import Iterator, Any, Optional, Deque, Dict, Tuple, Callable
from math import ceil
import logging
from collections import deque

from rich.live import Live
from rich.progress import (
    Progress,
    ProgressColumn,
    BarColumn,
    MofNCompleteColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    Task,
)
from rich.text import Text

from .execution import (
    ExecutionEventFormatter,
    ExecutionEventSink,
    ExecutionLogEvent,
)
from .execution_context import (
    reset_current_execution_event_sink,
    reset_current_visual_log_level,
    set_current_execution_event_sink,
    set_current_visual_log_level,
    visible_dag_indent,
)
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


def _styled_source_label(line: str) -> Text:
    text = Text()
    working = str(line or "")
    indent_len = len(working) - len(working.lstrip(" "))
    if indent_len:
        text.append(working[:indent_len])
    working = working[indent_len:]
    if working.startswith("[") and "]" in working:
        end = working.find("]")
        stream_id = working[1:end]
        rest = working[end + 1 :]
        text.append("[", style="cyan")
        text.append(stream_id, style="bold cyan")
        text.append("]", style="cyan")
        text.append(rest)
        return text
    text.append(working)
    return text


class SourceLabelColumn(ProgressColumn):
    def render(self, task: Task) -> Text:
        raw = task.fields.get("text", "")
        return _styled_source_label(str(raw))


def _is_stream_task(task: Task) -> bool:
    return str(task.fields.get("kind", "stream")) == "stream"


class GatedColumn(ProgressColumn):
    def __init__(
        self,
        inner: ProgressColumn,
        predicate: Callable[[Task], bool],
    ) -> None:
        self._inner = inner
        self._predicate = predicate
        super().__init__()

    def render(self, task: Task) -> Text:
        if not self._predicate(task):
            return Text("")
        return self._inner.render(task)

    def get_table_column(self):
        return self._inner.get_table_column()


class _RichSourceProxy(Source):
    def __init__(
        self,
        stream_source: Source,
        stream_id: str,
        progress: Progress,
        unit: Optional[str] = None,
    ):
        self._inner = stream_source
        self._stream_id = stream_id
        self._progress = progress
        self._task_id = None
        self._unit = unit
        self._emitted = 0

    def _format_text(self, stream_label: str, message: str) -> str:
        # Plain stream-id prefix to avoid Rich markup issues
        indent = visible_dag_indent(logging.INFO)
        return f"{indent}[{stream_label}] {message}" if message else f"{indent}[{stream_label}]"

    def stream(self) -> Iterator[Any]:
        adapter = SourceObservabilityAdapter(self._inner, self._stream_id)
        self._unit = adapter.unit
        stream_label = self._stream_id
        info_lines = adapter.info_lines()
        initial_message = adapter.format_label(
            include_stream_id=False,
            include_dag_indent=False,
        )

        for line in info_lines:
            self._progress.add_task(
                "",
                total=1,
                completed=1,
                text=self._format_text(stream_label, line),
                kind="meta",
                start=False,
            )

        self._task_id = self._progress.add_task(
            "",
            start=False,
            total=None,
            text=self._format_text(stream_label, initial_message),
            kind="stream",
        )

        if self._task_id is not None:
            total = adapter.count()
            if total is not None:
                self._progress.update(self._task_id, total=total)

        emitted = 0
        last_path_label: Optional[str] = None
        if self._task_id is not None:
            self._progress.start_task(self._task_id)

        try:
            for item in self._inner.stream():
                current_label = adapter.current_label()
                if current_label and current_label != last_path_label:
                    last_path_label = current_label
                    row_message = adapter.format_label(
                        current_label,
                        include_stream_id=False,
                        include_dag_indent=False,
                    )
                    text = self._format_text(
                        stream_label,
                        row_message,
                    )
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
            except Exception:
                logger.debug(
                    "visuals: failed to finalize progress task for %s",
                    self._stream_id,
                    exc_info=True,
                )
            # Defer logging of completion to the session footer to avoid interleaving
            self._emitted = emitted
            # No explicit end separator; completion line is sufficient


class _RichConsoleExecutionSink(ExecutionEventSink):
    def __init__(self, level: int, console, live_ref: dict[str, Live | None]) -> None:
        self._level = int(level)
        self._console = console
        self._live_ref = live_ref

    def emit(self, event: ExecutionLogEvent) -> None:
        event_level = ExecutionEventFormatter.level(event)
        if event_level < self._level:
            return
        text = self._render_event(event)
        live = self._live_ref.get("live")
        if live is not None:
            live.console.print(text)
            return
        self._console.print(text)

    def _render_event(self, event: ExecutionLogEvent) -> Text:
        indent = "  " * max(0, event.depth)
        text = Text(indent)
        if event.kind == "dag_info":
            text.append("[", style="cyan")
            text.append(event.dag_name, style="bold cyan")
            text.append("] ", style="cyan")
            text.append(event.info_line or "", style="dim")
            return text
        if event.kind == "dag_start":
            text.append("DAG started", style="bold cyan")
            text.append(f" name={event.dag_name} nodes={event.node_count}")
            return text
        if event.kind == "dag_end":
            status_style = "green" if event.status == "success" else "red"
            text.append("DAG finished", style="bold cyan")
            text.append(f" name={event.dag_name} ")
            text.append(f"status={event.status}", style=status_style)
            text.append(
                f" items={event.output_items} elapsed={event.elapsed_seconds:.6f}s"
            )
            return text
        if event.kind == "node_start":
            text.append("Node started", style="dim cyan")
            text.append(
                f" dag={event.dag_name} node={event.node_name} stage={event.stage}",
                style="dim",
            )
            return text
        status_style = "green" if event.status == "success" else "red"
        text.append("Node finished", style="dim cyan")
        text.append(
            f" dag={event.dag_name} node={event.node_name} stage={event.stage}",
            style="dim",
        )
        text.append(" ")
        text.append(f"status={event.status}", style=f"dim {status_style}")
        text.append(
            f" items={event.output_items} elapsed={event.elapsed_seconds:.6f}s",
            style="dim",
        )
        return text


@contextmanager
def visual_sources(runtime: Runtime, log_level: int | None):
    level = log_level if log_level is not None else logging.INFO

    # Build a console on stderr for visuals/logs
    from rich.console import Console as _Console
    import sys as _sys
    _vis_console = _Console(file=_sys.stderr, markup=False,
                            highlight=False, soft_wrap=True)

    columns = [
        SourceLabelColumn(),
        GatedColumn(BarColumn(), _is_stream_task),
        GatedColumn(MofNCompleteColumn(), _is_stream_task),
        GatedColumn(TaskProgressColumn(), _is_stream_task),
        GatedColumn(TimeElapsedColumn(), _is_stream_task),
        GatedColumn(AverageTimeRemainingColumn(), _is_stream_task),
    ]

    # Keep Live output transient so the spinner/bars disappear once completed
    progress = Progress(*columns, transient=True, console=_vis_console)
    live_ref: dict[str, Live | None] = {"live": None}

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())
    level_token = set_current_visual_log_level(level)
    execution_sink_token = set_current_execution_event_sink(
        _RichConsoleExecutionSink(level=level, console=_vis_console, live_ref=live_ref)
    )

    with Live(progress, console=_vis_console, refresh_per_second=10, transient=True) as live:
        try:
            live_ref["live"] = live
            for stream_id, stream_source in originals.items():
                proxy = _RichSourceProxy(
                    stream_source=stream_source,
                    stream_id=stream_id,
                    progress=progress,
                )
                reg.register(stream_id, proxy)
            yield
        finally:
            live_ref["live"] = None
            # Restore original sources
            for stream_id, stream_source in originals.items():
                reg.register(stream_id, stream_source)
            reset_current_execution_event_sink(execution_sink_token)
            reset_current_visual_log_level(level_token)
