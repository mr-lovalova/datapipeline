import logging
from contextlib import contextmanager
from dataclasses import dataclass
from collections.abc import Sequence
from typing import Any, Literal

from datapipeline.dag.events import DagParentRef, DagRunEvent, NodeExecutionEvent
from datapipeline.dag.node import NodeKind
from datapipeline.dag.observer import ExecutionObserver
from datapipeline.cli.visuals.execution_context import (
    current_execution_scope,
    reset_current_execution_scope,
    set_current_execution_scope,
    current_execution_event_sink,
    set_current_dag_depth,
)


ExecutionEventKind = Literal[
    "message",
    "dag_start",
    "dag_info",
    "dag_end",
    "node_start",
    "node_end",
]


@dataclass(frozen=True)
class ExecutionLogEvent:
    kind: ExecutionEventKind
    dag_name: str
    depth: int
    message: str | None = None
    message_kind: str | None = None
    log_level: int | None = None
    node_count: int | None = None
    node_name: str | None = None
    node_index: int | None = None
    execution_index: int | None = None
    node_kind: NodeKind = "function"
    node_calls_dag: str | None = None
    status: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    output_items: int | None = None
    elapsed_seconds: float | None = None
    info_line: str | None = None
    dag_parent: DagParentRef | None = None
    scope_profile_kind: str | None = None
    scope_profile_name: str | None = None
    scope_target_id: str | None = None
    scope_task_id: str | None = None
    scope_item_index: str | None = None
    scope_item_total: str | None = None


class ExecutionEventSink:
    def emit(self, event: ExecutionLogEvent) -> None:
        raise NotImplementedError


class ExecutionEventFormatter:
    @staticmethod
    def error_suffix(event: ExecutionLogEvent) -> str:
        if event.status != "error" or event.error_type is None:
            return ""
        suffix = f" error={event.error_type}"
        if event.error_message:
            message = event.error_message.replace("\n", "\\n")
            suffix = f"{suffix}: {message}"
        return suffix

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
        if event.kind in {"node_start", "node_end"}:
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
                    f" parent_node={event.dag_parent.node_name}"
                    f" parent_node_index={event.dag_parent.node_index}"
                )
            return (
                f"{indent}DAG started name={event.dag_name} "
                f"nodes={event.node_count}{parent_suffix}"
            )
        if event.kind == "dag_end":
            error_suffix = cls.error_suffix(event)
            return (
                f"{indent}DAG finished name={event.dag_name} "
                f"status={event.status}{error_suffix} items={event.output_items} "
                f"elapsed={event.elapsed_seconds:.6f}s"
            )
        if event.kind == "node_start":
            calls_suffix = (
                f" calls={event.node_calls_dag}"
                if event.node_calls_dag is not None
                else ""
            )
            return (
                f"{indent}Node execution started dag={event.dag_name} "
                f"node={event.node_name} index={event.node_index} "
                f"execution_index={event.execution_index} "
                f"kind={event.node_kind}{calls_suffix}"
            )
        error_suffix = cls.error_suffix(event)
        return (
            f"{indent}Node execution finished dag={event.dag_name} "
            f"node={event.node_name} index={event.node_index} kind={event.node_kind} "
            f"execution_index={event.execution_index} "
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
            "dp_node_count": event.node_count,
            "dp_node_name": event.node_name,
            "dp_index": event.node_index,
            "dp_execution_index": event.execution_index,
            "dp_node_kind": event.node_kind,
            "dp_node_calls_dag": event.node_calls_dag,
            "dp_status": event.status,
            "dp_error_type": event.error_type,
            "dp_error_message": event.error_message,
            "dp_output_items": event.output_items,
            "dp_elapsed_seconds": event.elapsed_seconds,
            "dp_info_line": event.info_line,
            "dp_parent_dag": (
                event.dag_parent.dag_name if event.dag_parent is not None else None
            ),
            "dp_parent_node": (
                event.dag_parent.node_name if event.dag_parent is not None else None
            ),
            "dp_parent_node_index": (
                event.dag_parent.node_index if event.dag_parent is not None else None
            ),
            "dp_scope_profile_kind": event.scope_profile_kind,
            "dp_scope_profile_name": event.scope_profile_name,
            "dp_scope_target_id": event.scope_target_id,
            "dp_scope_task_id": event.scope_task_id,
            "dp_scope_item_index": event.scope_item_index,
            "dp_scope_item_total": event.scope_item_total,
        }


def _scope_fields() -> dict[str, str | None]:
    scope = current_execution_scope() or {}
    return {
        "scope_profile_kind": scope.get("profile_kind"),
        "scope_profile_name": scope.get("profile_name"),
        "scope_target_id": scope.get("target_id"),
        "scope_task_id": scope.get("task_id"),
        "scope_item_index": scope.get("item_index"),
        "scope_item_total": scope.get("item_total"),
    }


def _scope_message(scope: dict[str, str]) -> str:
    task = scope.get("task_id") or scope.get("target_id") or scope.get("profile_name")
    if task is None:
        return "Scope"
    item = ""
    if scope.get("item_index") and scope.get("item_total"):
        item = f" ({scope['item_index']}/{scope['item_total']})"
    return f"Scope: task={task}{item}"


@contextmanager
def execution_scope(
    *,
    profile_kind: str | None = None,
    profile_name: str | None = None,
    target_id: str | None = None,
    task_id: str | None = None,
    item_index: int | None = None,
    item_total: int | None = None,
    announce: bool = False,
):
    merged = dict(current_execution_scope() or {})
    updates = {
        "profile_kind": profile_kind,
        "profile_name": profile_name,
        "target_id": target_id,
        "task_id": task_id,
        "item_index": item_index,
        "item_total": item_total,
    }
    for key, value in updates.items():
        if value is not None:
            merged[key] = str(value)

    token = set_current_execution_scope(merged)
    try:
        if announce:
            emit_execution_message(
                _scope_message(merged),
                level=logging.INFO,
                logger=logging.getLogger(__name__),
                message_kind="scope_start",
            )
        yield
    finally:
        reset_current_execution_scope(token)


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
        **_scope_fields(),
    )
    logger_sink = LoggerExecutionEventSink(logger or logging.getLogger(__name__))
    logger_sink.emit(event)
    ContextExecutionEventSink().emit(event)


def emit_source_info(
    stream_id: str,
    message: str,
    *,
    logger: logging.Logger | None = None,
    depth: int = 0,
) -> None:
    emit_execution_message(
        f"[{stream_id}] {message}",
        level=logging.INFO,
        logger=logger,
        depth=depth,
        message_kind="source_info",
    )


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
        
        dag_name: str,
        node_count: int,
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
                node_count=node_count,
                dag_parent=dag_parent,
                **_scope_fields(),
            )
        )
        for line in self._metadata_lines(dag_metadata):
            self._emit(
                ExecutionLogEvent(
                    kind="dag_info",
                    dag_name=dag_name,
                    depth=dag_depth + 1,
                    info_line=line,
                    **_scope_fields(),
                )
            )
        set_current_dag_depth(dag_depth + 1)

    def on_node_start(
        self,
        
        dag_name: str,
        node_name: str,
        node_index: int,
        execution_index: int,
        node_kind: NodeKind = "function",
        node_calls_dag: str | None = None,
        depth: int = 0,
    ) -> None:
        node_depth = max(0, int(depth))
        self._emit(
            ExecutionLogEvent(
                kind="node_start",
                dag_name=dag_name,
                depth=node_depth,
                node_name=node_name,
                node_index=node_index,
                execution_index=execution_index,
                node_kind=node_kind,
                node_calls_dag=node_calls_dag,
                **_scope_fields(),
            )
        )
        set_current_dag_depth(node_depth)

    def on_node_end(self, event: NodeExecutionEvent) -> None:
        node_depth = max(0, int(event.depth))
        self._emit(
            ExecutionLogEvent(
                kind="node_end",
                dag_name=event.dag_name,
                depth=node_depth,
                node_name=event.node_name,
                node_index=event.node_index,
                execution_index=event.execution_index,
                node_kind=event.node_kind,
                node_calls_dag=event.node_calls_dag,
                status=event.status,
                error_type=event.error_type,
                error_message=event.error_message,
                output_items=event.output_items,
                elapsed_seconds=event.elapsed_seconds,
                **_scope_fields(),
            )
        )
        set_current_dag_depth(node_depth)

    def on_dag_end(self, event: DagRunEvent) -> None:
        dag_depth = max(0, int(event.depth))
        self._emit(
            ExecutionLogEvent(
                kind="dag_end",
                dag_name=event.dag_name,
                depth=dag_depth,
                node_count=event.node_count,
                status=event.status,
                error_type=event.error_type,
                error_message=event.error_message,
                output_items=event.output_items,
                elapsed_seconds=event.elapsed_seconds,
                dag_parent=event.parent,
                **_scope_fields(),
            )
        )
        set_current_dag_depth(dag_depth)


def make_execution_observer(
    logger: logging.Logger | None = None,
    
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
