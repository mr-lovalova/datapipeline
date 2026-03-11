import logging
from contextlib import contextmanager

from rich.text import Text

from ..execution import ExecutionEventFormatter, ExecutionEventSink, ExecutionLogEvent
from ..execution_context import (
    reset_current_execution_event_sink,
    reset_current_visual_log_level,
    set_current_dag_depth,
    set_current_execution_event_sink,
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
        if self._live_console is not None:
            self._live_console.print(text)
            return
        self._console.print(text)

    def flush(self) -> None:
        return

    def _render_event(self, event: ExecutionLogEvent) -> Text:
        indent = "  " * max(0, event.depth)
        text = Text(indent)
        if event.kind == "message":
            style = "dim" if ExecutionEventFormatter.level(event) <= logging.DEBUG else ""
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
            if event.message_kind in {
                "build_decision",
                "profile_start",
            }:
                prefix, _, rest = message.partition("\n")
                text.append(prefix, style="bold cyan")
                if rest:
                    text.append(f"\n{rest}", style="dim")
                return text
            text.append(message, style=style)
            return text
        if event.kind == "dag_info":
            text.append("[", style="cyan")
            text.append(event.dag_name, style="bold cyan")
            text.append("] ", style="cyan")
            text.append(event.info_line or "", style="dim")
            return text
        if event.kind == "dag_start":
            text.append("DAG started", style="bold cyan")
            text.append(f" name={event.dag_name} steps={event.step_count}")
            if event.dag_parent is not None:
                text.append(
                    " "
                    f"parent_dag={event.dag_parent.dag_name} "
                    f"parent_step={event.dag_parent.step_name} "
                    f"parent_step_index={event.dag_parent.step_index}"
                )
            return text
        if event.kind == "dag_end":
            status_style = "green" if event.status == "success" else "red"
            text.append("DAG finished", style="bold cyan")
            text.append(f" name={event.dag_name} ")
            text.append(f"status={event.status}", style=status_style)
            if event.status == "error" and event.error_type:
                text.append(f" error={event.error_type}", style="red")
            text.append(f" items={event.output_items} elapsed={event.elapsed_seconds:.6f}s")
            return text
        if event.kind == "step_start":
            text.append("Step activated", style="dim cyan")
            text.append(
                (
                    f" dag={event.dag_name} step={event.step_name} "
                    f"index={event.step_index} kind={event.step_kind}"
                ),
                style="dim",
            )
            if event.step_calls_dag is not None:
                text.append(f" calls={event.step_calls_dag}", style="dim")
            return text
        status_style = "green" if event.status == "success" else "red"
        text.append("Step finished", style="dim cyan")
        text.append(
            (
                f" dag={event.dag_name} step={event.step_name} "
                f"index={event.step_index} kind={event.step_kind}"
            ),
            style="dim",
        )
        text.append(" ")
        text.append(f"status={event.status}", style=f"dim {status_style}")
        if event.status == "error" and event.error_type:
            text.append(f" error={event.error_type}", style="dim red")
        text.append(
            f" items={event.output_items} elapsed={event.elapsed_seconds:.6f}s",
            style="dim",
        )
        return text

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
    try:
        yield
    finally:
        sink.flush()
        reset_current_execution_event_sink(sink_token)
        reset_current_visual_log_level(level_token)
        # Guard against leaked depth from pre-task debug blocks.
        set_current_dag_depth(0)


__all__ = ["_RichConsoleExecutionSink", "visual_event_sink"]
