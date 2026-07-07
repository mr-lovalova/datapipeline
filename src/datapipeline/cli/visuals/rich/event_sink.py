import logging
from contextlib import contextmanager

from rich.text import Text

from ..execution import ExecutionEventFormatter, ExecutionEventSink, ExecutionLogEvent
from ..execution_context import (
    reset_current_execution_event_sink,
    reset_current_terminal_log_proxy_sink,
    reset_current_visual_log_level,
    set_current_dag_depth,
    set_current_execution_event_sink,
    set_current_terminal_log_proxy_sink,
    set_current_visual_log_level,
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
        if event.kind == "node_progress":
            console.print(text, overflow="ellipsis", no_wrap=True)
            return
        console.print(text)

    def flush(self) -> None:
        return

    def _render_event(self, event: ExecutionLogEvent) -> Text:
        indent = "  " * ExecutionEventFormatter.display_depth(event)
        text = Text(indent)
        if event.kind == "message":
            style = self._message_style(ExecutionEventFormatter.level(event))
            message = event.message or ""
            if "\n" in message:
                message = message.replace("\n", f"\n{indent}")
            if event.message_kind == "scope_start":
                scope_text = self._scope_header(event, message)
                if event.depth == 0:
                    text.append("\n")
                text.append("── ", style="cyan")
                text.append(scope_text, style="cyan")
                text.append(" ──", style="cyan")
                if event.depth == 0:
                    text.append("\n")
                return text
            if event.message_kind in {"saved", "materialized"}:
                prefix, _, rest = message.partition(" ")
                text.append(prefix, style="bold cyan")
                if rest:
                    text.append(f" {rest}", style=style)
                return text
            if event.message_kind == "source_info":
                text.append_text(styled_source_label(message))
                return text
            if event.message_kind == "build_decision":
                prefix, _, rest = message.partition("\n")
                text.append(prefix, style="bold cyan")
                if rest:
                    text.append(f"\n{rest}", style="dim")
                return text
            text.append(message, style=style)
            return text
        if event.kind == "dag_info":
            self._append_label(text, event.dag_name)
            style = "" if event.info_name == "source" else "dim"
            text.append(event.info_line or "", style=style)
            return text
        if event.kind == "dag_start":
            self._append_label(text, event.dag_name)
            text.append(f"started nodes={event.node_count}")
            return text
        if event.kind == "dag_end":
            status_style = "green" if event.status == "success" else "red"
            self._append_label(text, event.dag_name)
            text.append("finished ")
            text.append(f"status={event.status}", style=status_style)
            if error_suffix := ExecutionEventFormatter.error_suffix(event):
                text.append(error_suffix, style="red")
            text.append(f" items={event.output_items} elapsed={event.elapsed_seconds:.6f}s")
            return text
        if event.kind == "node_start":
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
        if event.kind == "node_progress":
            label = ExecutionEventFormatter.execution_label(
                event.dag_name,
                event.node_name,
            )
            self._append_debug_label(text, label)
            text.append(event.message or "", style="dim")
            return text
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
    def _scope_header(event: ExecutionLogEvent, fallback: str) -> str:
        task = event.scope_task_id or event.scope_target_id or event.scope_profile_name
        if task is None:
            return fallback or "Scope"
        header = f"Task: {task}"
        if event.scope_item_index and event.scope_item_total:
            header = f"{header} ({event.scope_item_index}/{event.scope_item_total})"
        return header


@contextmanager
def visual_event_sink(log_level: int | None):
    level = log_level if log_level is not None else logging.INFO
    from rich.console import Console as _Console
    import sys as _sys

    console = _Console(file=_sys.stderr, markup=False, highlight=False)
    sink = _RichConsoleExecutionSink(level=level, console=console)
    level_token = set_current_visual_log_level(level)
    sink_token = set_current_execution_event_sink(sink)
    proxy_token = set_current_terminal_log_proxy_sink(sink)
    try:
        yield
    finally:
        sink.flush()
        reset_current_terminal_log_proxy_sink(proxy_token)
        reset_current_execution_event_sink(sink_token)
        reset_current_visual_log_level(level_token)
        # Guard against leaked depth from pre-task debug blocks.
        set_current_dag_depth(0)


__all__ = ["_RichConsoleExecutionSink", "visual_event_sink"]
