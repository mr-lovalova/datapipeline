import logging

from rich.text import Text

from ..execution import (
    BuildDecisionMessage,
    DagFinished,
    DagInfo,
    DagStarted,
    ExecutionEventFormatter,
    ExecutionEventSink,
    ExecutionLogEvent,
    ExecutionMessage,
    NodeFinished,
    NodeProgress,
    NodeStarted,
    OperationFinished,
    OperationInfo,
    OperationProgress,
    OperationStarted,
    ProfileStartMessage,
    ScopeStartMessage,
    SourceInfoMessage,
)
from .columns import styled_source_label


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
        console = self._live_console or self._console
        if isinstance(event, NodeProgress | OperationProgress):
            console.print(text, overflow="ellipsis", no_wrap=True)
            return
        console.print(text)

    def flush(self) -> None:
        return

    def _render_event(self, event: ExecutionLogEvent) -> Text:
        if not isinstance(event, ExecutionLogEvent):
            raise TypeError(f"Unsupported execution event: {type(event).__name__}")

        indent = "  " * ExecutionEventFormatter.display_depth(event)
        text = Text(indent)

        if isinstance(event, ExecutionMessage | ProfileStartMessage):
            message = event.message
            if "\n" in message:
                message = message.replace("\n", f"\n{indent}")
            text.append(
                message,
                style=self._message_style(ExecutionEventFormatter.level(event)),
            )
            return text

        if isinstance(event, ScopeStartMessage):
            message = event.message
            if "\n" in message:
                message = message.replace("\n", f"\n{indent}")
            scope_text = self._scope_header(event, message)
            if event.depth == 0:
                text.append("\n")
            text.append("── ", style="cyan")
            text.append(scope_text, style="cyan")
            text.append(" ──", style="cyan")
            if event.depth == 0:
                text.append("\n")
            return text

        if isinstance(event, SourceInfoMessage):
            message = f"[{event.source_label}] {event.message}"
            if "\n" in message:
                message = message.replace("\n", f"\n{indent}")
            text.append_text(styled_source_label(message))
            return text

        if isinstance(event, BuildDecisionMessage):
            message = event.message
            if "\n" in message:
                message = message.replace("\n", f"\n{indent}")
            prefix, _, rest = message.partition("\n")
            text.append(prefix, style="bold cyan")
            if rest:
                text.append(f"\n{rest}", style="dim")
            return text

        if isinstance(event, DagInfo):
            self._append_label(text, event.dag_name)
            style = "" if event.info_name == "source" else "dim"
            text.append(event.info_line, style=style)
            return text

        if isinstance(event, OperationInfo):
            self._append_label(text, event.operation_name)
            text.append(event.info_line)
            return text

        if isinstance(event, DagStarted):
            self._append_label(text, event.dag_name)
            text.append("started")
            text.append(f" nodes={event.node_count}")
            return text

        if isinstance(event, OperationStarted):
            self._append_label(text, event.operation_name)
            text.append("started")
            text.append(f" operation={event.entrypoint}")
            return text

        if isinstance(event, DagFinished):
            status_style = "green" if event.status == "success" else "red"
            self._append_label(text, event.dag_name)
            text.append("finished ")
            text.append(f"status={event.status}", style=status_style)
            if error_suffix := ExecutionEventFormatter.error_suffix(event):
                text.append(error_suffix, style="red")
            text.append(f" items={event.output_items}")
            text.append(f" elapsed={event.elapsed_seconds:.6f}s")
            return text

        if isinstance(event, OperationFinished):
            status_style = "green" if event.status == "success" else "red"
            self._append_label(text, event.operation_name)
            text.append("finished ")
            text.append(f"status={event.status}", style=status_style)
            if error_suffix := ExecutionEventFormatter.error_suffix(event):
                text.append(error_suffix, style="red")
            text.append(f" elapsed={event.elapsed_seconds:.6f}s")
            return text

        if isinstance(event, NodeStarted):
            label = ExecutionEventFormatter.execution_label(
                event.dag_name,
                event.node_name,
            )
            self._append_debug_label(text, label)
            text.append(
                (
                    f"started index={event.node_index} "
                    f"execution={event.execution_index} kind={event.node_kind}"
                ),
                style="dim",
            )
            if event.node_calls_dag is not None:
                text.append(f" calls={event.node_calls_dag}", style="dim")
            return text

        if isinstance(event, NodeProgress):
            label = ExecutionEventFormatter.execution_label(
                event.dag_name,
                event.node_name,
            )
            self._append_debug_label(text, label)
            text.append(event.message, style="dim")
            return text

        if isinstance(event, OperationProgress):
            label = ExecutionEventFormatter.execution_label(
                event.operation_name,
                event.step,
            )
            self._append_debug_label(text, label)
            text.append(event.message, style="dim")
            return text

        if isinstance(event, NodeFinished):
            status_style = "green" if event.status == "success" else "red"
            label = ExecutionEventFormatter.execution_label(
                event.dag_name,
                event.node_name,
            )
            self._append_debug_label(text, label)
            text.append(
                (
                    f"finished index={event.node_index} "
                    f"execution={event.execution_index} kind={event.node_kind}"
                ),
                style="dim",
            )
            text.append(" ")
            text.append(f"status={event.status}", style=f"dim {status_style}")
            if error_suffix := ExecutionEventFormatter.error_suffix(event):
                text.append(error_suffix, style="dim red")
            text.append(
                f" items={event.output_items} elapsed={event.elapsed_seconds:.6f}s",
                style="dim",
            )
            return text

        raise TypeError(f"Unsupported execution event: {type(event).__name__}")

    @staticmethod
    def _append_label(text: Text, label: str) -> None:
        text.append("[", style="cyan")
        text.append(label, style="bold cyan")
        text.append("] ", style="cyan")

    @staticmethod
    def _append_debug_label(text: Text, label: str) -> None:
        text.append("[", style="dim cyan")
        text.append(label, style="dim bold cyan")
        text.append("] ", style="dim cyan")

    @staticmethod
    def _message_style(level: int) -> str:
        if level >= logging.ERROR:
            return "bold red"
        if level >= logging.WARNING:
            return "yellow"
        if level <= logging.DEBUG:
            return "dim"
        return ""

    @staticmethod
    def _scope_header(event: ScopeStartMessage, fallback: str) -> str:
        task = event.scope.task_id or event.scope.target_id or event.scope.profile_name
        if task is None:
            return fallback or "Scope"
        header = f"Task: {task}"
        if event.scope.item_index and event.scope.item_total:
            header = f"{header} ({event.scope.item_index}/{event.scope.item_total})"
        return header


__all__ = ["_RichConsoleExecutionSink"]
