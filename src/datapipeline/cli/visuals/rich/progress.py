import logging
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    Task,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Column
from rich.text import Text

from datapipeline.cli.visuals.execution import (
    PipelineFinished,
    PipelineStarted,
    ExecutionLogEvent,
    NodeFinished,
    NodeProgress,
    NodeStarted,
)
from datapipeline.cli.visuals.execution_context import (
    reset_current_execution_event_sink,
    reset_current_terminal_log_proxy_sink,
    set_current_execution_event_sink,
    set_current_terminal_log_proxy_sink,
)
from datapipeline.execution.events import ProgressSnapshot

from .event_sink import _RichConsoleExecutionSink


@dataclass
class _NodeState:
    label: str
    task_id: TaskID | None = None
    progress: ProgressSnapshot | None = None


class _LiveElapsedColumn(TimeElapsedColumn):
    """Show root pipeline time across completed local progress phases."""

    def render(self, task: Task) -> Text:
        if not task.fields["show_elapsed"]:
            return Text()
        elapsed = timedelta(seconds=max(0, int(task.elapsed or 0)))
        return Text(f"PIPELINE {elapsed}", style="dim")


class _ExecutionProgress:
    def __init__(self, progress: Progress, debug: bool) -> None:
        self._progress = progress
        self._debug = debug
        self._root_pipeline: str | None = None
        self._root_task: TaskID | None = None
        self._nodes: dict[int, _NodeState] = {}
        self._active_nodes: list[int] = []

    def handle(self, event: ExecutionLogEvent) -> None:
        if isinstance(event, PipelineStarted):
            self._start_pipeline(event)
        elif isinstance(event, NodeStarted):
            self._start_node(event)
        elif isinstance(event, NodeProgress):
            self._update_node(event)
        elif isinstance(event, NodeFinished):
            self._finish_node(event)
        elif isinstance(event, PipelineFinished):
            self._finish_pipeline(event)
        else:
            raise TypeError(f"Unsupported progress event: {type(event).__name__}")

    def clear(self) -> None:
        for task in list(self._progress.tasks):
            self._progress.remove_task(task.id)
        self._root_pipeline = None
        self._root_task = None
        self._nodes.clear()
        self._active_nodes.clear()

    def _start_pipeline(self, event: PipelineStarted) -> None:
        if self._root_pipeline is not None:
            raise RuntimeError("Cannot start overlapping pipeline progress")
        self._root_pipeline = event.pipeline_name
        self._root_task = self._progress.add_task(
            event.pipeline_name,
            total=None,
            status="",
            indent="",
            show_elapsed=True,
        )

    def _finish_pipeline(self, event: PipelineFinished) -> None:
        if self._root_pipeline is None:
            raise RuntimeError("Cannot finish pipeline progress before it starts")
        if self._root_pipeline != event.pipeline_name:
            raise RuntimeError("Pipeline progress finished out of order")
        self.clear()

    def _start_node(self, event: NodeStarted) -> None:
        if self._root_pipeline is None:
            raise RuntimeError("Cannot start node progress before its root pipeline")
        label = f"{event.pipeline_name}/{event.node_name}"
        state = _NodeState(label=label)
        if self._debug:
            state.task_id = self._progress.add_task(
                label,
                total=None,
                status="0 out",
                indent="",
                show_elapsed=False,
            )
        self._nodes[event.node_index] = state
        self._active_nodes.append(event.node_index)
        if not self._debug:
            self._render_active_node()

    def _update_node(self, event: NodeProgress) -> None:
        state = self._nodes[event.node_index]
        state.progress = event.progress
        if self._debug:
            if state.task_id is not None:
                self._update_task(
                    state.task_id,
                    completed=event.progress.completed,
                    total=event.progress.total,
                    status=_progress_status(event.progress),
                )
        elif (
            event.persistent
            or event.progress.total is not None
            or event.progress.phase is not None
            or event.progress.detail is not None
            or event.progress.resource is not None
            or event.progress.unit not in ("items", "out")
            or self._active_nodes[-1] == event.node_index
        ):
            self._render_info_node(event.node_index)

    def _finish_node(self, event: NodeFinished) -> None:
        state = self._nodes.pop(event.node_index)
        self._active_nodes.remove(event.node_index)
        if state.task_id is not None:
            self._progress.remove_task(state.task_id)
        if not self._debug:
            self._render_active_node()

    def _render_active_node(self) -> None:
        if self._root_task is None:
            raise RuntimeError("Cannot render node progress before its root pipeline")
        if not self._active_nodes:
            self._update_task(
                self._root_task,
                total=None,
                completed=0,
                status="finishing",
            )
            return
        self._render_info_node(self._active_nodes[-1])

    def _render_info_node(self, node_index: int) -> None:
        if self._root_task is None:
            raise RuntimeError("Cannot render node progress before its root pipeline")
        state = self._nodes[node_index]
        if state.progress is None:
            self._update_task(
                self._root_task,
                total=None,
                completed=0,
                status=f"{state.label} · 0 out",
            )
            return
        self._update_task(
            self._root_task,
            completed=state.progress.completed,
            total=state.progress.total,
            status=f"{state.label} · {_progress_status(state.progress)}",
        )

    def _update_task(
        self,
        task_id: TaskID,
        completed: int,
        total: int | None,
        status: str,
    ) -> None:
        task = next(task for task in self._progress.tasks if task.id == task_id)
        task.total = total
        self._progress.update(task_id, completed=completed, status=status)


def _progress_status(snapshot: ProgressSnapshot) -> str:
    resource = snapshot.resource
    parts = [snapshot.phase] if snapshot.phase else []
    if resource is not None:
        parts.append(f"{resource.index}/{resource.total} {resource.label}")
    if snapshot.detail:
        parts.append(snapshot.detail)
    count = f"{snapshot.completed:,}"
    if snapshot.total is not None:
        count = f"{count}/{snapshot.total:,}"
    parts.append(f"{count} {snapshot.unit}")
    return " · ".join(parts)


@contextmanager
def visual_execution(log_level: int):
    console = Console(file=sys.stderr, markup=False, highlight=False)
    debug = log_level <= logging.DEBUG
    progress = Progress(
        TextColumn(
            "{task.fields[indent]}[{task.description}]",
            markup=False,
            table_column=Column(no_wrap=True, overflow="ellipsis"),
        ),
        BarColumn(
            bar_width=20,
            style="grey30",
            complete_style="cyan",
            finished_style="cyan",
            pulse_style="cyan",
            table_column=Column(no_wrap=True),
        ),
        _LiveElapsedColumn(table_column=Column(no_wrap=True)),
        TextColumn(
            "{task.fields[status]}",
            markup=False,
            table_column=Column(no_wrap=True, overflow="ellipsis"),
        ),
        transient=True,
        console=console,
        refresh_per_second=10,
    )
    renderer = _ExecutionProgress(progress, debug=debug)
    sink = _RichConsoleExecutionSink(log_level, console, renderer)
    event_token = set_current_execution_event_sink(sink)
    proxy_token = set_current_terminal_log_proxy_sink(sink)
    try:
        with progress:
            try:
                yield
            finally:
                renderer.clear()
    finally:
        reset_current_terminal_log_proxy_sink(proxy_token)
        reset_current_execution_event_sink(event_token)
