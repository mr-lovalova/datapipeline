import logging
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskProgressColumn,
    TimeElapsedColumn,
)

from datapipeline.runtime import Runtime
from datapipeline.sources.models.source import Source

from ..execution import emit_execution_message
from ..execution_context import (
    current_dag_depth,
    reset_current_execution_event_sink,
    reset_current_terminal_log_proxy_sink,
    reset_current_visual_log_level,
    set_current_dag_depth,
    set_current_execution_event_sink,
    set_current_terminal_log_proxy_sink,
    set_current_visual_log_level,
    visible_dag_indent,
)
from ..source_observability import SourceObservabilityAdapter
from .columns import AverageTimeRemainingColumn, SourceLabelColumn
from .event_sink import _RichConsoleExecutionSink, visual_event_sink

logger = logging.getLogger(__name__)


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
        return f"{indent}[{stream_label}] {message}" if message else f"{indent}[{stream_label}]"

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
        self._safe_progress_call("stop progress task", self._progress.stop_task, task_id)
        if remove:
            # Remove completed stream rows so finished bars don't linger
            # while other sources are still running in the same Live table.
            self._safe_progress_call("remove progress task", self._progress.remove_task, task_id)

    def stream(self) -> Iterator[Any]:
        adapter = SourceObservabilityAdapter(self._inner, self._stream_id)
        stream_label = self._stream_id
        task_id: Optional[int] = None
        sequence_entries = getattr(adapter, "progress_sequence", lambda: None)()
        sequence_index = 0
        task_emitted = 0
        retired_task_ids: list[int] = []
        info_lines = [str(line).strip() for line in adapter.info_lines() if str(line).strip()]
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
            self._safe_progress_call("refresh progress", self._progress.refresh)

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
                                text=self._format_text(stream_label, row_message),
                                total=next_total,
                            )
                        else:
                            self._progress.update(
                                task_id,
                                text=self._format_text(stream_label, row_message),
                            )
                        # File transitions can be brief; refresh immediately so
                        # short-lived labels (e.g. first file in a sequence) render.
                        self._safe_progress_call("refresh progress", self._progress.refresh)
                if task_id is not None:
                    self._progress.advance(task_id, 1)
                    task_emitted += 1
                    if not refreshed_after_start:
                        refreshed_after_start = True
                        self._safe_progress_call("refresh progress", self._progress.refresh)
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
    execution_sink = _RichConsoleExecutionSink(level=level, console=_vis_console)

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())
    level_token = set_current_visual_log_level(level)
    execution_sink_token = set_current_execution_event_sink(execution_sink)
    proxy_token = set_current_terminal_log_proxy_sink(execution_sink)

    try:
        with progress:
            try:
                execution_sink.set_live_console(progress.live.console if progress.live else None)
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
            reset_current_terminal_log_proxy_sink(proxy_token)
            reset_current_execution_event_sink(execution_sink_token)
            reset_current_visual_log_level(level_token)


__all__ = [
    "_RichConsoleExecutionSink",
    "_RichSourceProxy",
    "_clear_progress_tasks",
    "AverageTimeRemainingColumn",
    "SourceLabelColumn",
    "visual_event_sink",
    "visual_sources",
]
