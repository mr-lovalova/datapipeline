import logging
from dataclasses import dataclass
from collections.abc import Sequence
from typing import Any, Literal

from datapipeline.dag.events import DagParentRef, DagRunEvent, StepRunEvent
from datapipeline.dag.node import StepKind
from datapipeline.dag.observer import ExecutionObserver
from datapipeline.cli.visuals.execution_context import (
    current_execution_event_sink,
    set_current_dag_depth,
)


ExecutionEventKind = Literal[
    "message",
    "dag_start",
    "dag_info",
    "dag_end",
    "step_start",
    "step_end",
]


@dataclass(frozen=True)
class ExecutionLogEvent:
    kind: ExecutionEventKind
    dag_name: str
    depth: int
    message: str | None = None
    message_kind: str | None = None
    log_level: int | None = None
    step_count: int | None = None
    step_name: str | None = None
    step_index: int | None = None
    step_kind: StepKind = "function"
    step_calls_dag: str | None = None
    status: str | None = None
    error_type: str | None = None
    output_items: int | None = None
    elapsed_seconds: float | None = None
    info_line: str | None = None
    dag_parent: DagParentRef | None = None


class ExecutionEventSink:
    def emit(self, event: ExecutionLogEvent) -> None:
        raise NotImplementedError


class ExecutionEventFormatter:
    @staticmethod
    def indent(depth: int) -> str:
        return "  " * max(0, depth)

    @staticmethod
    def level(event: ExecutionLogEvent) -> int:
        if event.kind == "message":
            return int(event.log_level) if event.log_level is not None else logging.INFO
        if event.kind in {"dag_start", "dag_end"}:
            # Keep DAG lifecycle visible at INFO, regardless of nesting depth.
            return logging.INFO
        if event.kind == "dag_info":
            info_line = str(event.info_line or "")
            if info_line.startswith("feature.config:") or info_line.startswith(
                "feature.transforms:"
            ):
                return logging.INFO
            if event.depth <= 1:
                return logging.INFO
            return logging.DEBUG
        if event.kind in {"step_start", "step_end"}:
            return logging.DEBUG
        return logging.INFO

    @classmethod
    def message(cls, event: ExecutionLogEvent) -> str:
        indent = cls.indent(event.depth)
        if event.kind == "message":
            message = event.message or ""
            if not indent or "\n" not in message:
                return f"{indent}{message}"
            return f"{indent}" + message.replace("\n", f"\n{indent}")
        if event.kind == "dag_info":
            return f"{indent}[{event.dag_name}] {event.info_line or ''}"
        if event.kind == "dag_start":
            parent_suffix = ""
            if event.dag_parent is not None:
                parent_suffix = (
                    f" parent_dag={event.dag_parent.dag_name}"
                    f" parent_step={event.dag_parent.step_name}"
                    f" parent_step_index={event.dag_parent.step_index}"
                )
            return (
                f"{indent}DAG started name={event.dag_name} "
                f"steps={event.step_count}{parent_suffix}"
            )
        if event.kind == "dag_end":
            error_suffix = (
                f" error={event.error_type}"
                if event.status == "error" and event.error_type
                else ""
            )
            return (
                f"{indent}DAG finished name={event.dag_name} "
                f"status={event.status}{error_suffix} items={event.output_items} "
                f"elapsed={event.elapsed_seconds:.6f}s"
            )
        if event.kind == "step_start":
            calls_suffix = (
                f" calls={event.step_calls_dag}"
                if event.step_calls_dag is not None
                else ""
            )
            return (
                f"{indent}Step activated dag={event.dag_name} "
                f"step={event.step_name} index={event.step_index} "
                f"kind={event.step_kind}{calls_suffix}"
            )
        error_suffix = (
            f" error={event.error_type}"
            if event.status == "error" and event.error_type
            else ""
        )
        return (
            f"{indent}Step finished dag={event.dag_name} "
            f"step={event.step_name} index={event.step_index} kind={event.step_kind} "
            f"status={event.status}{error_suffix} items={event.output_items} "
            f"elapsed={event.elapsed_seconds:.6f}s"
        )

    @staticmethod
    def extra(event: ExecutionLogEvent) -> dict[str, object]:
        return {
            "dp_event_kind": event.kind,
            "dp_dag_name": event.dag_name,
            "dp_depth": event.depth,
            "dp_message": event.message,
            "dp_message_kind": event.message_kind,
            "dp_log_level": event.log_level,
            "dp_step_count": event.step_count,
            "dp_step_name": event.step_name,
            "dp_index": event.step_index,
            "dp_step_kind": event.step_kind,
            "dp_step_calls_dag": event.step_calls_dag,
            "dp_status": event.status,
            "dp_error_type": event.error_type,
            "dp_output_items": event.output_items,
            "dp_elapsed_seconds": event.elapsed_seconds,
            "dp_info_line": event.info_line,
            "dp_parent_dag": (
                event.dag_parent.dag_name if event.dag_parent is not None else None
            ),
            "dp_parent_step": (
                event.dag_parent.step_name if event.dag_parent is not None else None
            ),
            "dp_parent_step_index": (
                event.dag_parent.step_index if event.dag_parent is not None else None
            ),
        }


class CompositeExecutionEventSink(ExecutionEventSink):
    def __init__(self, sinks: Sequence[ExecutionEventSink]) -> None:
        self._sinks = tuple(sinks)

    def emit(self, event: ExecutionLogEvent) -> None:
        for sink in self._sinks:
            sink.emit(event)


class LoggerExecutionEventSink(ExecutionEventSink):
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def emit(self, event: ExecutionLogEvent) -> None:
        level = ExecutionEventFormatter.level(event)
        if not self._logger.isEnabledFor(level):
            return
        self._logger.log(
            level,
            ExecutionEventFormatter.message(event),
            extra=ExecutionEventFormatter.extra(event),
        )


class ContextExecutionEventSink(ExecutionEventSink):
    """Optional additive sink bound in execution context (used by visuals)."""

    def emit(self, event: ExecutionLogEvent) -> None:
        sink = current_execution_event_sink()
        if sink is None or sink is self:
            return
        sink.emit(event)


def emit_execution_message(
    message: str,
    *,
    level: int = logging.INFO,
    logger: logging.Logger | None = None,
    depth: int = 0,
    message_kind: str | None = None,
) -> None:
    event = ExecutionLogEvent(
        kind="message",
        dag_name="",
        depth=max(0, int(depth)),
        message=message,
        message_kind=message_kind,
        log_level=int(level),
    )
    logger_sink = LoggerExecutionEventSink(logger or logging.getLogger(__name__))
    logger_sink.emit(event)
    ContextExecutionEventSink().emit(event)


class HierarchicalExecutionObserver(ExecutionObserver):
    def __init__(self, sink: ExecutionEventSink) -> None:
        self._sink = sink
        set_current_dag_depth(0)

    def _emit(self, event: ExecutionLogEvent) -> None:
        self._sink.emit(event)

    @staticmethod
    def _metadata_lines(dag_metadata: dict[str, Any] | None) -> list[str]:
        if not dag_metadata:
            return []
        lines: list[str] = []
        for key, value in dag_metadata.items():
            if isinstance(value, dict):
                parts = [f"{k}={value[k]}" for k in value]
                lines.append(f"{key}: {' '.join(parts)}")
            else:
                lines.append(f"{key}: {value}")
        return lines

    def on_dag_start(
        self,
        *,
        dag_name: str,
        step_count: int,
        depth: int = 0,
        dag_metadata: dict[str, Any] | None = None,
        dag_parent: DagParentRef | None = None,
    ) -> None:
        dag_depth = max(0, int(depth))
        self._emit(
            ExecutionLogEvent(
                kind="dag_start",
                dag_name=dag_name,
                depth=dag_depth,
                step_count=step_count,
                dag_parent=dag_parent,
            )
        )
        for line in self._metadata_lines(dag_metadata):
            self._emit(
                ExecutionLogEvent(
                    kind="dag_info",
                    dag_name=dag_name,
                    depth=dag_depth + 1,
                    info_line=line,
                )
            )
        set_current_dag_depth(dag_depth + 1)

    def on_step_start(
        self,
        *,
        dag_name: str,
        step_name: str,
        step_index: int,
        step_kind: StepKind = "function",
        step_calls_dag: str | None = None,
        depth: int = 0,
    ) -> None:
        step_depth = max(0, int(depth))
        self._emit(
            ExecutionLogEvent(
                kind="step_start",
                dag_name=dag_name,
                depth=step_depth,
                step_name=step_name,
                step_index=step_index,
                step_kind=step_kind,
                step_calls_dag=step_calls_dag,
            )
        )
        set_current_dag_depth(step_depth)

    def on_step_end(self, event: StepRunEvent) -> None:
        step_depth = max(0, int(event.depth))
        self._emit(
            ExecutionLogEvent(
                kind="step_end",
                dag_name=event.dag_name,
                depth=step_depth,
                step_name=event.step_name,
                step_index=event.step_index,
                step_kind=event.step_kind,
                step_calls_dag=event.step_calls_dag,
                status=event.status,
                error_type=event.error_type,
                output_items=event.output_items,
                elapsed_seconds=event.elapsed_seconds,
            )
        )
        set_current_dag_depth(step_depth)

    def on_dag_end(self, event: DagRunEvent) -> None:
        dag_depth = max(0, int(event.depth))
        self._emit(
            ExecutionLogEvent(
                kind="dag_end",
                dag_name=event.dag_name,
                depth=dag_depth,
                step_count=event.step_count,
                status=event.status,
                error_type=event.error_type,
                output_items=event.output_items,
                elapsed_seconds=event.elapsed_seconds,
                dag_parent=event.parent,
            )
        )
        set_current_dag_depth(dag_depth)


def make_execution_observer(
    logger: logging.Logger | None = None,
    *,
    sink: ExecutionEventSink | None = None,
    sinks: Sequence[ExecutionEventSink] | None = None,
) -> ExecutionObserver:
    if sink is not None and sinks is not None:
        raise ValueError("Pass either 'sink' or 'sinks', not both")

    sink_list: list[ExecutionEventSink]
    if sink is not None:
        sink_list = [sink]
    elif sinks is not None:
        sink_list = list(sinks)
    else:
        logger_sink = LoggerExecutionEventSink(logger or logging.getLogger(__name__))
        sink_list = [logger_sink, ContextExecutionEventSink()]

    if not sink_list:
        raise ValueError("'sinks' must contain at least one sink")

    active_sink: ExecutionEventSink
    if len(sink_list) == 1:
        active_sink = sink_list[0]
    else:
        active_sink = CompositeExecutionEventSink(sink_list)
    return HierarchicalExecutionObserver(active_sink)
