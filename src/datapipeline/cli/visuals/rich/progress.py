import logging
import sys
from contextlib import contextmanager
from datetime import timedelta

from rich.console import Console, RenderableType
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    Task,
    TaskID,
)
from rich.rule import Rule
from rich.table import Column, Table
from rich.text import Text

from datapipeline.cli.visuals.execution import (
    ExecutionEventFormatter,
    ExecutionLogEvent,
)
from datapipeline.cli.visuals.execution_context import (
    reset_current_execution_event_handler,
    reset_current_terminal_log_handler,
    set_current_execution_event_handler,
    set_current_terminal_log_handler,
)
from datapipeline.execution.events import (
    NodeFinished,
    NodeProgress,
    NodeStarted,
    PipelineFinished,
    PipelineProgress,
    PipelineStarted,
    ProgressSnapshot,
)
from datapipeline.execution.observability import (
    CommandFinished,
    FileResult,
    OperationFinished,
    OperationProgress,
    OperationStarted,
)


class _ProgressLabelColumn(ProgressColumn):
    def render(self, task: Task) -> RenderableType:
        label = Text(task.description)
        elapsed = timedelta(seconds=int(task.elapsed or 0))
        label.append(f" {elapsed}")
        return label


class _ProgressActivityColumn(ProgressColumn):
    def __init__(self, table_column: Column) -> None:
        super().__init__(table_column)
        self._bar = BarColumn(
            bar_width=20,
            style="grey30",
            complete_style="cyan",
            finished_style="cyan",
            pulse_style="cyan",
        )

    def render(self, task: Task) -> RenderableType:
        status = Text(task.fields["status"])
        if task.total is None:
            return status
        activity = Table.grid(padding=(0, 1))
        activity.add_row(self._bar.render(task), status)
        return activity


class _ExecutionProgress:
    def __init__(self, progress: Progress, debug: bool) -> None:
        self._progress = progress
        self._debug = debug
        self._operation_name: str | None = None
        self._operation_task: TaskID | None = None
        self._pipeline_name: str | None = None
        self._pipeline_task: TaskID | None = None
        self._node_tasks: dict[int, TaskID] = {}
        self._open_nodes: list[int] = []
        self._visible_node: int | None = None

    def handle(self, event: ExecutionLogEvent) -> None:
        if isinstance(event, OperationStarted):
            self._start_operation(event)
        elif isinstance(event, OperationProgress):
            self._update_operation(event)
        elif isinstance(event, OperationFinished):
            self._finish_operation(event)
        elif isinstance(event, PipelineStarted):
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
        self._clear_pipeline()
        if self._operation_task is not None:
            self._progress.remove_task(self._operation_task)
        self._operation_name = None
        self._operation_task = None
        self._progress.refresh()

    def _start_operation(self, event: OperationStarted) -> None:
        if self._operation_name is not None:
            raise RuntimeError("Cannot start overlapping operation progress")
        self._operation_name = event.name
        self._operation_task = self._progress.add_task(
            "Elapsed",
            total=None,
            status="",
        )

    def _update_operation(self, event: OperationProgress) -> None:
        if self._operation_name is None or self._operation_task is None:
            raise RuntimeError("Cannot update operation progress before it starts")
        if self._operation_name != event.name:
            raise RuntimeError("Operation progress updated out of order")
        self._progress.update(
            self._operation_task,
            completed=event.completed,
            status=f"{event.step} · {event.completed:,} {event.unit}",
        )

    def _finish_operation(self, event: OperationFinished) -> None:
        if self._operation_name is None or self._operation_task is None:
            raise RuntimeError("Cannot finish operation progress before it starts")
        if self._operation_name != event.name:
            raise RuntimeError("Operation progress finished out of order")
        self.clear()

    def _start_pipeline(self, event: PipelineStarted) -> None:
        if self._pipeline_name is not None:
            raise RuntimeError("Cannot start overlapping pipeline progress")
        self._pipeline_name = event.pipeline_name
        self._pipeline_task = self._progress.add_task(
            f"[{event.pipeline_name}]",
            total=None,
            status="",
        )

    def _finish_pipeline(self, event: PipelineFinished) -> None:
        if self._pipeline_name is None:
            raise RuntimeError("Cannot finish pipeline progress before it starts")
        if self._pipeline_name != event.pipeline_name:
            raise RuntimeError("Pipeline progress finished out of order")
        self._clear_pipeline()
        self._progress.refresh()

    def _start_node(self, event: NodeStarted) -> None:
        if self._pipeline_name is None:
            raise RuntimeError("Cannot start node progress before its root pipeline")
        label = f"[{event.pipeline_name}/{event.node_name}]"
        self._node_tasks[event.node_index] = self._progress.add_task(
            label,
            total=None,
            status="0 out",
            visible=self._debug,
        )
        self._open_nodes.append(event.node_index)
        if not self._debug:
            self._show_node(event.node_index)

    def _update_node(self, event: NodeProgress) -> None:
        self._update_task(
            self._node_tasks[event.node_index],
            completed=event.progress.completed,
            total=event.progress.total,
            status=_progress_status(event.progress),
        )
        if not self._debug and (
            event.heartbeat
            or event.progress.total is not None
            or event.progress.phase is not None
            or event.progress.detail is not None
            or event.progress.resource is not None
            or event.progress.unit not in ("items", "out")
            or self._open_nodes[-1] == event.node_index
        ):
            self._show_node(event.node_index)

    def _finish_node(self, event: NodeFinished) -> None:
        task_id = self._node_tasks.pop(event.node_index)
        self._open_nodes.remove(event.node_index)
        was_visible = self._visible_node == event.node_index
        if was_visible:
            self._visible_node = None
        self._progress.remove_task(task_id)
        if not self._debug and was_visible and self._open_nodes:
            self._show_node(self._open_nodes[-1])
        self._progress.refresh()

    def _show_node(self, node_index: int) -> None:
        if self._visible_node == node_index:
            return
        if self._visible_node is not None:
            self._progress.update(
                self._node_tasks[self._visible_node],
                visible=False,
            )
        self._progress.update(self._node_tasks[node_index], visible=True)
        self._visible_node = node_index

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

    def _clear_pipeline(self) -> None:
        for task_id in self._node_tasks.values():
            self._progress.remove_task(task_id)
        if self._pipeline_task is not None:
            self._progress.remove_task(self._pipeline_task)
        self._pipeline_name = None
        self._pipeline_task = None
        self._node_tasks.clear()
        self._open_nodes.clear()
        self._visible_node = None


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


class _RichExecutionRenderer:
    def __init__(
        self,
        level: int,
        console,
        progress: _ExecutionProgress | None = None,
    ) -> None:
        self._level = int(level)
        self._console = console
        self._progress = progress

    def render(self, event: ExecutionLogEvent) -> None:
        if self._progress is not None and isinstance(event, PipelineProgress):
            return
        if self._progress is not None and isinstance(
            event,
            OperationStarted
            | OperationProgress
            | OperationFinished
            | PipelineStarted
            | NodeStarted
            | NodeProgress
            | NodeFinished
            | PipelineFinished,
        ):
            self._progress.handle(event)
            if isinstance(event, OperationStarted):
                self._console.print(Rule(Text(f"Operation {event.name}"), style="dim"))
                return
            if isinstance(
                event,
                OperationProgress | NodeStarted | NodeProgress,
            ):
                return
        if isinstance(event, NodeProgress):
            return
        event_level = ExecutionEventFormatter.level(event)
        if event_level < self._level:
            return
        if isinstance(event, FileResult):
            self._console.print(self._render_file_result(event))
            return
        text = self._render_event(event)
        self._console.print(text)

    @staticmethod
    def _render_file_result(event: FileResult) -> Table:
        table = Table.grid(padding=(0, 1))
        table.add_column(no_wrap=True)
        table.add_column(ratio=1, overflow="fold")
        result = Text(str(event.path))
        result.stylize(f"blue link {event.path.resolve().as_uri()}")
        table.add_row(f"{event.label}:", result)
        return table

    def _render_event(self, event: ExecutionLogEvent) -> Text:
        level = ExecutionEventFormatter.level(event)
        text = Text(ExecutionEventFormatter.message(event))
        if isinstance(
            event,
            CommandFinished | PipelineFinished | NodeFinished | OperationFinished,
        ):
            status_style = "green" if event.status == "success" else "red"
            text.highlight_words([f"status={event.status}"], style=status_style)
        elif level >= logging.ERROR:
            text.stylize("red")
        elif level >= logging.WARNING:
            text.stylize("yellow")
        return text


def rich_visuals_supported() -> bool:
    if not sys.stderr.isatty():
        return False
    console = Console(file=sys.stderr, markup=False, highlight=False)
    return bool(
        console.is_terminal
        and console.is_interactive
        and not console.is_dumb_terminal
        and console.color_system is not None
    )


@contextmanager
def visual_execution(log_level: int):
    console = Console(file=sys.stderr, markup=False, highlight=False)
    debug = log_level <= logging.DEBUG
    progress = Progress(
        _ProgressLabelColumn(Column(no_wrap=True, overflow="ellipsis")),
        _ProgressActivityColumn(Column(no_wrap=True, overflow="ellipsis")),
        transient=True,
        console=console,
        refresh_per_second=10,
    )
    progress_renderer = _ExecutionProgress(progress, debug=debug)
    event_renderer = _RichExecutionRenderer(log_level, console, progress_renderer)
    event_token = set_current_execution_event_handler(event_renderer.render)
    proxy_token = set_current_terminal_log_handler(event_renderer.render)
    refresh_thread = None
    try:
        with progress:
            # Rich clears this reference when stopping without joining the thread.
            refresh_thread = progress.live._refresh_thread
            try:
                yield
            finally:
                progress_renderer.clear()
    finally:
        if refresh_thread is not None:
            # Do not let its FileProxy survive into interpreter shutdown.
            refresh_thread.join()
        reset_current_terminal_log_handler(proxy_token)
        reset_current_execution_event_handler(event_token)
