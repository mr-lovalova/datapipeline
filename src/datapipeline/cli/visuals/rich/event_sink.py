import logging
from typing import Protocol

from rich.text import Text

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
            if not isinstance(event, DagFinished):
                return
        if isinstance(event, NodeProgress):
            return
        event_level = ExecutionEventFormatter.level(event)
        if event_level < self._level:
            return
        text = self._render_event(event)
        if isinstance(event, OperationProgress):
            self._console.print(text, overflow="ellipsis", no_wrap=True)
            return
        self._console.print(text)

    def _render_event(self, event: ExecutionLogEvent) -> Text:
        if not isinstance(event, ExecutionLogEvent):
            raise TypeError(f"Unsupported execution event: {type(event).__name__}")
        level = ExecutionEventFormatter.level(event)
        return Text(
            ExecutionEventFormatter.message(event),
            style=self._message_style(level),
        )

    @staticmethod
    def _message_style(level: int) -> str:
        if level >= logging.ERROR:
            return "bold red"
        if level >= logging.WARNING:
            return "yellow"
        if level <= logging.DEBUG:
            return "dim"
        return ""
