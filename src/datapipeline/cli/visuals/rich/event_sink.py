import logging
from typing import Protocol

from rich.table import Table
from rich.text import Text

from datapipeline.execution.observability import (
    FileResult,
    OperationFinished,
    format_record_count,
)

from ..execution import (
    DagFinished,
    DagStarted,
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
            DagStarted | NodeStarted | NodeProgress | NodeFinished | DagFinished,
        ):
            self._progress_renderer.handle(event)
            if isinstance(event, NodeStarted | NodeProgress):
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
        if isinstance(event, OperationProgress):
            self._console.print(text, overflow="ellipsis", no_wrap=True)
            return
        self._console.print(text)

    @staticmethod
    def _render_file_result(event: FileResult) -> Table:
        table = Table.grid(padding=(0, 1))
        table.add_column(no_wrap=True, style="bold white")
        table.add_column(ratio=1, overflow="fold")
        result = Text()
        result.append(
            str(event.path),
            style=f"bright_blue link {event.path.resolve().as_uri()}",
        )
        records = "" if event.records is None else format_record_count(event.records)
        result.append(f" · {records}" if records else "", style="dim")
        table.add_row(f"{event.label}:", result)
        return table

    def _render_event(self, event: ExecutionLogEvent) -> Text:
        if not isinstance(event, ExecutionLogEvent):
            raise TypeError(f"Unsupported execution event: {type(event).__name__}")
        level = ExecutionEventFormatter.level(event)
        message = ExecutionEventFormatter.message(event)
        text = Text(
            message,
            style=self._message_style(level),
        )
        text.highlight_regex(r"^\s*\[[^]]+\]", style="bold cyan")
        if isinstance(event, DagFinished | NodeFinished | OperationFinished):
            status_style = "green" if event.status == "success" else "bold red"
            text.highlight_words([f"status={event.status}"], style=status_style)
        return text

    @staticmethod
    def _message_style(level: int) -> str:
        if level >= logging.ERROR:
            return "bold red"
        if level >= logging.WARNING:
            return "yellow"
        if level <= logging.DEBUG:
            return "dim"
        return ""
