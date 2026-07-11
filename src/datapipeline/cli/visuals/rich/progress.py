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
    label: str
    task_id: TaskID | None = None
    progress: ProgressSnapshot | None = None


class _LiveElapsedColumn(TimeElapsedColumn):
    """Measure execution lifetime across completed local progress phases."""

    def render(self, task: Task) -> Text:
        elapsed = task.elapsed
        if elapsed is None:
            return Text("-:--:--", style="progress.elapsed")
        return Text(
            str(timedelta(seconds=max(0, int(elapsed)))),
            style="progress.elapsed",
        )


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
                indent="",
                timer_scope="DAG",
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
        if self._root_dag is None:
            raise RuntimeError("Cannot start node progress before its root DAG")
        label = f"{event.dag_name}/{event.node_name}"
        state = _NodeState(label=label)
        if self._debug:
            relative_depth = max(0, event.depth - self._root_dag[1] - 1)
            state.task_id = self._progress.add_task(
                label,
                total=None,
                status="starting",
                indent="  " * relative_depth,
                timer_scope="NODE",
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
        elif (
            event.persistent
            or event.progress.total is not None
            or event.progress.phase is not None
            or event.progress.detail is not None
            or event.progress.resource is not None
            or event.progress.unit != "items"
            or self._active_nodes[-1] == event.execution_index
        ):
            self._render_info_node(event.execution_index)

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
            self._progress.update(
                self._root_task,
                description=self._root_dag[0],
            )
            self._update_task(
                self._root_task,
                total=None,
                completed=0,
                status="finishing",
            )
            return
        self._render_info_node(self._active_nodes[-1])

    def _render_info_node(self, execution_index: int) -> None:
        if self._root_task is None:
            raise RuntimeError("Cannot render node progress before its root DAG")
        state = self._nodes[execution_index]
        self._progress.update(self._root_task, description=state.label)
        if state.progress is None:
            self._update_task(
                self._root_task,
                total=None,
                completed=0,
                status="starting",
            )
            return
        self._render(self._root_task, state.progress)

    def _render(self, task_id: TaskID, snapshot: ProgressSnapshot) -> None:
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
        self._update_task(
            task_id,
            completed=snapshot.completed,
            total=snapshot.total,
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
    debug = log_level <= logging.DEBUG
    progress = Progress(
        TextColumn(
            "{task.fields[indent]}[{task.description}]",
            style="bold cyan",
            markup=False,
            table_column=Column(no_wrap=True, overflow="ellipsis"),
        ),
        BarColumn(
            bar_width=20,
            style="grey30",
            complete_style="cyan",
            finished_style="green",
            pulse_style="cyan",
            table_column=Column(no_wrap=True),
        ),
        TextColumn(
            "{task.fields[timer_scope]}",
            style="dim",
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
        set_current_dag_depth(0)
        reset_current_terminal_log_proxy_sink(proxy_token)
        reset_current_execution_event_sink(event_token)
