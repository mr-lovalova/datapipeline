import logging
import sys
from contextlib import contextmanager
from dataclasses import dataclass

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Column

from datapipeline.cli.visuals.execution import (
    DagFinished,
    DagStarted,
    ExecutionLogEvent,
    NodeFinished,
    NodeProgress,
    NodeStarted,
)
from datapipeline.cli.visuals.execution_context import (
    reset_current_execution_event_sink,
    reset_current_terminal_log_proxy_sink,
    set_current_dag_depth,
    set_current_execution_event_sink,
    set_current_terminal_log_proxy_sink,
)
from datapipeline.dag.events import ProgressSnapshot

from .event_sink import _RichConsoleExecutionSink


@dataclass
class _NodeState:
    dag_name: str
    node_name: str
    task_id: TaskID | None = None
    progress: ProgressSnapshot | None = None


class _ExecutionProgress:
    def __init__(self, progress: Progress, debug: bool) -> None:
        self._progress = progress
        self._debug = debug
        self._root_dag: tuple[str, int] | None = None
        self._root_task: TaskID | None = None
        self._nodes: dict[int, _NodeState] = {}
        self._active_nodes: list[int] = []

    def handle(self, event: ExecutionLogEvent) -> None:
        if isinstance(event, DagStarted):
            self._start_dag(event)
        elif isinstance(event, NodeStarted):
            self._start_node(event)
        elif isinstance(event, NodeProgress):
            self._update_node(event)
        elif isinstance(event, NodeFinished):
            self._finish_node(event)
        elif isinstance(event, DagFinished):
            self._finish_dag(event)
        else:
            raise TypeError(f"Unsupported progress event: {type(event).__name__}")

    def clear(self) -> None:
        for task in list(self._progress.tasks):
            self._progress.remove_task(task.id)
        self._root_dag = None
        self._root_task = None
        self._nodes.clear()
        self._active_nodes.clear()

    def _start_dag(self, event: DagStarted) -> None:
        if self._root_dag is not None:
            if event.depth <= self._root_dag[1]:
                raise RuntimeError("Cannot start overlapping root DAG progress")
            return
        self._root_dag = event.dag_name, event.depth
        if not self._debug:
            self._root_task = self._progress.add_task(
                event.dag_name,
                total=None,
                status="starting",
            )

    def _finish_dag(self, event: DagFinished) -> None:
        if self._root_dag is None:
            raise RuntimeError("Cannot finish DAG progress before it starts")
        if event.depth > self._root_dag[1]:
            return
        if self._root_dag != (event.dag_name, event.depth):
            raise RuntimeError("Root DAG progress finished out of order")
        self.clear()

    def _start_node(self, event: NodeStarted) -> None:
        state = _NodeState(event.dag_name, event.node_name)
        if self._debug:
            state.task_id = self._progress.add_task(
                f"{event.dag_name}/{event.node_name}",
                total=None,
                status="starting",
            )
        self._nodes[event.execution_index] = state
        self._active_nodes.append(event.execution_index)
        if not self._debug:
            self._render_active_node()

    def _update_node(self, event: NodeProgress) -> None:
        state = self._nodes[event.execution_index]
        state.progress = event.progress
        if self._debug:
            if state.task_id is not None:
                self._render(state.task_id, event.progress)
        elif self._active_nodes and self._active_nodes[-1] == event.execution_index:
            self._render_active_node()

    def _finish_node(self, event: NodeFinished) -> None:
        state = self._nodes.pop(event.execution_index)
        self._active_nodes.remove(event.execution_index)
        if state.task_id is not None:
            self._progress.remove_task(state.task_id)
        if not self._debug:
            self._render_active_node()

    def _render_active_node(self) -> None:
        if self._root_task is None or self._root_dag is None:
            raise RuntimeError("Cannot render node progress before its root DAG")
        if not self._active_nodes:
            self._update_task(
                self._root_task,
                total=None,
                completed=0,
                status="finishing",
            )
            return
        state = self._nodes[self._active_nodes[-1]]
        root_dag_name = self._root_dag[0]
        label = (
            state.node_name
            if state.dag_name == root_dag_name
            else f"{state.dag_name}/{state.node_name}"
        )
        if state.progress is None:
            self._update_task(
                self._root_task,
                total=None,
                completed=0,
                status=f"{label} · starting",
            )
            return
        self._render(self._root_task, state.progress, prefix=label)

    def _render(
        self,
        task_id: TaskID,
        snapshot: ProgressSnapshot,
        prefix: str | None = None,
    ) -> None:
        resource = snapshot.resource
        if snapshot.total is not None:
            total = snapshot.total
            completed = snapshot.completed
        elif resource is not None:
            total = resource.total
            completed = max(0, resource.index - 1)
        else:
            total = None
            completed = snapshot.completed

        parts = [part for part in (prefix, snapshot.phase) if part]
        if resource is not None:
            parts.append(f"{resource.index}/{resource.total} {resource.label}")
        if snapshot.detail:
            parts.append(snapshot.detail)
        count = f"{snapshot.completed:,}"
        if snapshot.total is not None:
            count = f"{count}/{snapshot.total:,}"
        parts.append(f"{count} {snapshot.unit}")
        self._update_task(
            task_id,
            completed=completed,
            total=total,
            status=" · ".join(parts),
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


@contextmanager
def visual_execution(log_level: int):
    console = Console(file=sys.stderr, markup=False, highlight=False)
    progress = Progress(
        TextColumn(
            "[{task.description}]",
            markup=False,
            table_column=Column(no_wrap=True, overflow="ellipsis"),
        ),
        BarColumn(bar_width=20, table_column=Column(no_wrap=True)),
        TimeElapsedColumn(table_column=Column(no_wrap=True)),
        TextColumn(
            "{task.fields[status]}",
            markup=False,
            table_column=Column(no_wrap=True, overflow="ellipsis"),
        ),
        transient=True,
        console=console,
        refresh_per_second=10,
    )
    renderer = _ExecutionProgress(progress, debug=log_level <= logging.DEBUG)
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
        set_current_dag_depth(0)
        reset_current_terminal_log_proxy_sink(proxy_token)
        reset_current_execution_event_sink(event_token)
