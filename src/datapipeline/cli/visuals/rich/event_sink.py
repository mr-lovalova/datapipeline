import logging
from typing import Protocol

from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from datapipeline.execution.observability import (
    FileResult,
    OperationFinished,
    OperationStarted,
)

from ..execution import (
    PipelineFinished,
    PipelineStarted,
    ExecutionEventFormatter,
    ExecutionEventSink,
    ExecutionLogEvent,
    NodeFinished,
    NodeProgress,
    NodeStarted,
    OperationProgress,
)


class _ProgressRenderer(Protocol):
    def handle(self, event: ExecutionLogEvent) -> None: ...


class _RichConsoleExecutionSink(ExecutionEventSink):
    def __init__(
        self,
        level: int,
        console,
        progress_renderer: _ProgressRenderer | None = None,
    ) -> None:
        self._level = int(level)
        self._console = console
        self._progress_renderer = progress_renderer

    def emit(self, event: ExecutionLogEvent) -> None:
        if self._progress_renderer is not None and isinstance(
            event,
            PipelineStarted
            | NodeStarted
            | NodeProgress
            | NodeFinished
            | PipelineFinished,
        ):
            self._progress_renderer.handle(event)
            if isinstance(event, NodeStarted | NodeProgress):
                return
        if isinstance(event, NodeProgress):
            return
        event_level = ExecutionEventFormatter.level(event)
        if event_level < self._level:
            return
        if isinstance(event, OperationStarted):
            self._console.print(Rule(Text(f"Operation {event.name}"), style="dim"))
            return
        if isinstance(event, FileResult):
            self._console.print(self._render_file_result(event))
            return
        text = self._render_event(event)
        if isinstance(event, OperationProgress):
            self._console.print(text, overflow="ellipsis", no_wrap=True)
            return
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
        if isinstance(event, PipelineFinished | NodeFinished | OperationFinished):
            status_style = "green" if event.status == "success" else "red"
            text.highlight_words([f"status={event.status}"], style=status_style)
        elif level >= logging.ERROR:
            text.stylize("red")
        elif level >= logging.WARNING:
            text.stylize("yellow")
        return text
