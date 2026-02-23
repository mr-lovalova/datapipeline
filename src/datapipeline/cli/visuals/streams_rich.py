from contextlib import contextmanager
from typing import Iterator, Any, Optional, Deque, Dict, Tuple
from math import ceil
import logging
from collections import deque

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
from rich.table import Column

from .execution import (
    ExecutionEventFormatter,
    ExecutionEventSink,
    ExecutionLogEvent,
    emit_execution_message,
)
from .execution_context import (
    current_dag_depth,
    reset_current_execution_event_sink,
    reset_current_visual_log_level,
    set_current_dag_depth,
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
        avg_seconds_per_item = recent if recent is not None else (
            elapsed / completed)
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
        rest = working[end + 1:]
        text.append("[", style="cyan")
        text.append(stream_id, style="bold cyan")
        text.append("]", style="cyan")
        text.append(rest)
        return text
    text.append(working)
    return text


class SourceLabelColumn(ProgressColumn):
    def get_table_column(self):
        # Keep one physical terminal line per task row so Live transient cleanup
        # can reliably clear the rendered table at context exit.
        return Column(no_wrap=True, overflow="ellipsis")

    def render(self, task: Task) -> Text:
        raw = task.fields.get("text", "")
        return _styled_source_label(str(raw))


class _RichSourceProxy(Source):
    def __init__(
        self,
        stream_source: Source,
        stream_id: str,
        progress: Progress,
    ):
        self._inner = stream_source
        self._stream_id = stream_id
        self._progress = progress

    def _safe_progress_call(self, operation: str, fn, *args, **kwargs) -> None:
        try:
            fn(*args, **kwargs)
        except KeyError:
            # Common during interrupt teardown when centralized cleanup removes
            # tasks before stream-local finalizers run.
            return
        except Exception:
            logger.debug(
                "visuals: failed to %s for %s",
                operation,
                self._stream_id,
                exc_info=True,
            )

    def _format_text(self, stream_label: str, message: str) -> str:
        # Compute indent at stream update time (inside DAG context), then keep
        # it embedded in task text so Live rendering remains context-accurate.
        indent = visible_dag_indent(logging.INFO)
        return (
            f"{indent}[{stream_label}] {message}"
            if message
            else f"{indent}[{stream_label}]"
        )

    def _start_task(self, *, text: str, total: Optional[int]) -> int:
        task_id = self._progress.add_task(
            "",
            start=False,
            total=total,
            text=text,
        )
        self._progress.start_task(task_id)
        return int(task_id)

    def _finalize_task(self, *, task_id: int, completed: int, remove: bool = True) -> None:
        self._safe_progress_call(
            "finalize progress task",
            self._progress.update,
            task_id,
            completed=completed,
        )
        self._safe_progress_call("stop progress task",
                                 self._progress.stop_task, task_id)
        if remove:
            # Remove completed stream rows so finished bars don't linger
            # while other sources are still running in the same Live table.
            self._safe_progress_call(
                "remove progress task", self._progress.remove_task, task_id)

    def stream(self) -> Iterator[Any]:
        adapter = SourceObservabilityAdapter(self._inner, self._stream_id)
        stream_label = self._stream_id
        task_id: Optional[int] = None
        sequence_entries = getattr(
            adapter, "progress_sequence", lambda: None)()
        sequence_index = 0
        task_emitted = 0
        retired_task_ids: list[int] = []
        info_lines = [str(line).strip()
                      for line in adapter.info_lines() if str(line).strip()]
        initial_message = adapter.format_label(
            include_stream_id=False,
            include_dag_indent=False,
        )
        initial_path_label = adapter.initial_label()
        if initial_path_label:
            initial_message = adapter.format_label(
                initial_path_label,
                include_stream_id=False,
                include_dag_indent=False,
            )
        for line in info_lines:
            emit_execution_message(
                f"[{stream_label}] {line}",
                level=logging.INFO,
                logger=logger,
                depth=current_dag_depth(),
                message_kind="source_info",
            )

        emitted = 0
        refreshed_after_start = False
        last_path_label: Optional[str] = initial_path_label

        try:
            total = adapter.count()
            if sequence_entries:
                total = sequence_entries[0].total
            task_id = self._start_task(
                text=self._format_text(stream_label, initial_message),
                total=total,
            )
            self._safe_progress_call(
                "refresh progress", self._progress.refresh)

            for item in self._inner.stream():
                current_label = adapter.current_label()
                if current_label and current_label != last_path_label:
                    last_path_label = current_label
                    row_message = adapter.format_label(
                        current_label,
                        include_stream_id=False,
                        include_dag_indent=False,
                    )
                    if task_id is not None:
                        if sequence_entries:
                            self._finalize_task(
                                task_id=task_id,
                                completed=task_emitted,
                                remove=False,
                            )
                            retired_task_ids.append(task_id)
                            task_emitted = 0
                            sequence_index += 1
                            next_total: Optional[int] = None
                            if 0 <= sequence_index < len(sequence_entries):
                                next_total = sequence_entries[sequence_index].total
                            task_id = self._start_task(
                                text=self._format_text(
                                    stream_label, row_message),
                                total=next_total,
                            )
                        else:
                            self._progress.update(
                                task_id,
                                text=self._format_text(
                                    stream_label, row_message),
                            )
                        # File transitions can be brief; refresh immediately so
                        # short-lived labels (e.g. first file in a sequence) render.
                        self._safe_progress_call(
                            "refresh progress", self._progress.refresh)
                if task_id is not None:
                    self._progress.advance(task_id, 1)
                    task_emitted += 1
                    if not refreshed_after_start:
                        refreshed_after_start = True
                        self._safe_progress_call(
                            "refresh progress", self._progress.refresh)
                emitted += 1
                yield item
        finally:
            if task_id is not None:
                self._finalize_task(
                    task_id=int(task_id),
                    completed=task_emitted if sequence_entries else emitted,
                    remove=False if sequence_entries else True,
                )
                if sequence_entries:
                    retired_task_ids.append(int(task_id))
            for retired_task_id in retired_task_ids:
                self._safe_progress_call(
                    "remove progress task",
                    self._progress.remove_task,
                    retired_task_id,
                )
            emit_execution_message(
                f"[{stream_label}] Stream complete items={emitted}",
                level=logging.INFO,
                logger=logger,
                depth=current_dag_depth(),
                message_kind="source_info",
            )


def _clear_progress_tasks(progress: Progress) -> None:
    # Keep cleanup centralized at the context boundary so interrupted streams
    # don't leave orphan rows behind.
    for task in list(progress.tasks):
        try:
            progress.remove_task(task.id)
        except Exception:
            logger.debug(
                "visuals: failed to remove orphan progress task %s",
                task.id,
                exc_info=True,
            )


class _RichConsoleExecutionSink(ExecutionEventSink):
    def __init__(self, level: int, console) -> None:
        self._level = int(level)
        self._console = console
        self._live_console = None

    def set_live_console(self, live_console) -> None:
        self._live_console = live_console

    def emit(self, event: ExecutionLogEvent) -> None:
        event_level = ExecutionEventFormatter.level(event)
        if event_level < self._level:
            return
        text = self._render_event(event)
        if self._live_console is not None:
            self._live_console.print(text)
            return
        self._console.print(text)

    def flush(self) -> None:
        return

    def _render_event(self, event: ExecutionLogEvent) -> Text:
        indent = "  " * max(0, event.depth)
        text = Text(indent)
        if event.kind == "message":
            style = "dim" if ExecutionEventFormatter.level(
                event) <= logging.DEBUG else ""
            message = event.message or ""
            if "\n" in message:
                message = message.replace("\n", f"\n{indent}")
            if event.message_kind in {"saved", "materialized"}:
                prefix, _, rest = message.partition(" ")
                text.append(prefix, style="bold cyan")
                if rest:
                    text.append(f" {rest}", style=style)
                return text
            if event.message_kind == "source_info":
                text.append_text(_styled_source_label(message))
                return text
            if event.message_kind in {"build_settings", "task_config"}:
                prefix, _, rest = message.partition("\n")
                text.append(prefix, style="bold cyan")
                if rest:
                    text.append(f"\n{rest}", style="dim")
                return text
            text.append(message, style=style)
            return text
        if event.kind == "dag_info":
            text.append("[", style="cyan")
            text.append(event.dag_name, style="bold cyan")
            text.append("] ", style="cyan")
            text.append(event.info_line or "", style="dim")
            return text
        if event.kind == "dag_start":
            text.append("DAG started", style="bold cyan")
            text.append(f" name={event.dag_name} steps={event.step_count}")
            if event.dag_parent is not None:
                text.append(
                    " "
                    f"parent_dag={event.dag_parent.dag_name} "
                    f"parent_step={event.dag_parent.step_name} "
                    f"parent_step_index={event.dag_parent.step_index}"
                )
            return text
        if event.kind == "dag_end":
            status_style = "green" if event.status == "success" else "red"
            text.append("DAG finished", style="bold cyan")
            text.append(f" name={event.dag_name} ")
            text.append(f"status={event.status}", style=status_style)
            if event.status == "error" and event.error_type:
                text.append(f" error={event.error_type}", style="red")
            text.append(
                f" items={event.output_items} elapsed={event.elapsed_seconds:.6f}s"
            )
            return text
        if event.kind == "step_start":
            text.append("Step activated", style="dim cyan")
            text.append(
                (
                    f" dag={event.dag_name} step={event.step_name} "
                    f"index={event.step_index} kind={event.step_kind}"
                ),
                style="dim",
            )
            if event.step_calls_dag is not None:
                text.append(f" calls={event.step_calls_dag}", style="dim")
            return text
        status_style = "green" if event.status == "success" else "red"
        text.append("Step finished", style="dim cyan")
        text.append(
            (
                f" dag={event.dag_name} step={event.step_name} "
                f"index={event.step_index} kind={event.step_kind}"
            ),
            style="dim",
        )
        text.append(" ")
        text.append(f"status={event.status}", style=f"dim {status_style}")
        if event.status == "error" and event.error_type:
            text.append(f" error={event.error_type}", style="dim red")
        text.append(
            f" items={event.output_items} elapsed={event.elapsed_seconds:.6f}s",
            style="dim",
        )
        return text


@contextmanager
def visual_event_sink(log_level: int | None):
    level = log_level if log_level is not None else logging.INFO
    from rich.console import Console as _Console
    import sys as _sys

    console = _Console(file=_sys.stderr, markup=False, highlight=False)
    sink = _RichConsoleExecutionSink(level=level, console=console)
    level_token = set_current_visual_log_level(level)
    sink_token = set_current_execution_event_sink(sink)
    try:
        yield
    finally:
        sink.flush()
        reset_current_execution_event_sink(sink_token)
        reset_current_visual_log_level(level_token)
        # Guard against leaked depth from pre-task debug blocks.
        set_current_dag_depth(0)


@contextmanager
def visual_sources(runtime: Runtime, log_level: int | None):
    level = log_level if log_level is not None else logging.INFO

    # Build a console on stderr for visuals/logs
    from rich.console import Console as _Console
    import sys as _sys
    _vis_console = _Console(file=_sys.stderr, markup=False, highlight=False)

    # Keep Live output transient so the spinner/bars disappear once completed
    progress = Progress(
        SourceLabelColumn(),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        AverageTimeRemainingColumn(),
        transient=True,
        console=_vis_console,
        refresh_per_second=10,
    )
    execution_sink = _RichConsoleExecutionSink(
        level=level, console=_vis_console)

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())
    level_token = set_current_visual_log_level(level)
    execution_sink_token = set_current_execution_event_sink(
        execution_sink
    )

    try:
        with progress:
            try:
                execution_sink.set_live_console(
                    progress.live.console if progress.live else None)
                for stream_id, stream_source in originals.items():
                    proxy = _RichSourceProxy(
                        stream_source=stream_source,
                        stream_id=stream_id,
                        progress=progress,
                    )
                    reg.register(stream_id, proxy)
                yield
            finally:
                _clear_progress_tasks(progress)
                # Restore original sources
                for stream_id, stream_source in originals.items():
                    reg.register(stream_id, stream_source)
    finally:
        execution_sink.set_live_console(None)
        set_current_dag_depth(0)
        try:
            execution_sink.flush()
        finally:
            reset_current_execution_event_sink(execution_sink_token)
            reset_current_visual_log_level(level_token)
