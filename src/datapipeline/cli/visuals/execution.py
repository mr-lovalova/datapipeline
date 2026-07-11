import logging
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Protocol

from datapipeline.cli.visuals.execution_context import (
    current_dag_label,
    current_execution_event_sink,
    current_execution_scope,
    reset_current_execution_scope,
    set_current_dag_depth,
    set_current_dag_label,
    set_current_execution_scope,
)
from datapipeline.dag.events import (
    DagParentRef,
    DagRunEvent,
    format_node_progress,
    NodeExecutionEvent,
    NodeProgressEvent as DagNodeProgressEvent,
    ProgressSnapshot,
    RunStatus,
)
from datapipeline.dag.node import NodeKind
from datapipeline.dag.observer import ExecutionObserver
from datapipeline.dag.runner import current_node_progress_context
from datapipeline.execution.observability import (
    OperationEvent as ObservedOperationEvent,
    OperationFinished as ObservedOperationFinished,
    OperationInfo as ObservedOperationInfo,
    OperationProgressEvent as ObservedOperationProgress,
    OperationStarted as ObservedOperationStarted,
    OperationStatus,
)


@dataclass(frozen=True)
class ExecutionScope:
    profile_kind: str | None = None
    profile_name: str | None = None
    target_id: str | None = None
    task_id: str | None = None
    item_index: str | None = None
    item_total: str | None = None


@dataclass(frozen=True, kw_only=True)
class _ExecutionEvent:
    depth: int = 0
    scope: ExecutionScope = field(default_factory=ExecutionScope)


@dataclass(frozen=True, kw_only=True)
class ExecutionMessage(_ExecutionEvent):
    message: str
    log_level: int = logging.INFO


@dataclass(frozen=True, kw_only=True)
class ProfileStarted(_ExecutionEvent):
    command: str
    name: str
    index: int
    total: int


@dataclass(frozen=True, kw_only=True)
class BuildDecisionMessage(_ExecutionEvent):
    message: str


@dataclass(frozen=True, kw_only=True)
class SourceInfoMessage(_ExecutionEvent):
    source_label: str
    message: str


@dataclass(frozen=True, kw_only=True)
class DagStarted(_ExecutionEvent):
    dag_name: str
    node_count: int
    dag_parent: DagParentRef | None = None


@dataclass(frozen=True, kw_only=True)
class DagInfo(_ExecutionEvent):
    dag_name: str
    info_name: str
    info_line: str


@dataclass(frozen=True, kw_only=True)
class DagFinished(_ExecutionEvent):
    dag_name: str
    node_count: int
    status: RunStatus
    output_items: int
    elapsed_seconds: float
    error_type: str | None = None
    error_message: str | None = None
    dag_parent: DagParentRef | None = None


@dataclass(frozen=True, kw_only=True)
class _NodeEvent(_ExecutionEvent):
    dag_name: str
    node_name: str
    node_index: int
    execution_index: int
    node_kind: NodeKind = "function"
    node_calls_dag: str | None = None


@dataclass(frozen=True, kw_only=True)
class NodeStarted(_NodeEvent):
    pass


@dataclass(frozen=True, kw_only=True)
class NodeProgress(_NodeEvent):
    progress: ProgressSnapshot
    elapsed_seconds: float
    persistent: bool = False


@dataclass(frozen=True, kw_only=True)
class NodeFinished(_NodeEvent):
    status: RunStatus
    output_items: int
    elapsed_seconds: float
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, kw_only=True)
class _OperationEvent(_ExecutionEvent):
    operation_name: str


@dataclass(frozen=True, kw_only=True)
class OperationStarted(_OperationEvent):
    entrypoint: str


@dataclass(frozen=True, kw_only=True)
class OperationInfo(_OperationEvent):
    info_line: str


@dataclass(frozen=True, kw_only=True)
class OperationProgress(_OperationEvent):
    step: str
    message: str


@dataclass(frozen=True, kw_only=True)
class OperationFinished(_OperationEvent):
    status: OperationStatus
    elapsed_seconds: float
    error_type: str | None = None
    error_message: str | None = None


ExecutionLogEvent = (
    ExecutionMessage
    | ProfileStarted
    | BuildDecisionMessage
    | SourceInfoMessage
    | DagStarted
    | DagInfo
    | DagFinished
    | NodeStarted
    | NodeProgress
    | NodeFinished
    | OperationStarted
    | OperationInfo
    | OperationProgress
    | OperationFinished
)


class ExecutionEventSink(Protocol):
    def emit(self, event: ExecutionLogEvent) -> None: ...


class ExecutionEventFormatter:
    @staticmethod
    def error_suffix(
        event: DagFinished | NodeFinished | OperationFinished,
    ) -> str:
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
    def display_depth(event: ExecutionLogEvent) -> int:
        if isinstance(event, NodeStarted | NodeProgress | NodeFinished):
            return max(0, int(event.depth) - 1)
        return max(0, int(event.depth))

    @staticmethod
    def execution_label(dag_name: str, node_name: str | None) -> str:
        if node_name:
            return f"{dag_name}/{node_name}"
        return dag_name

    @staticmethod
    def progress_message(event: NodeProgress) -> str:
        return format_node_progress(event.progress, event.elapsed_seconds)

    @staticmethod
    def level(event: ExecutionLogEvent) -> int:
        if isinstance(event, ExecutionMessage):
            return int(event.log_level)
        if isinstance(event, DagFinished | NodeFinished | OperationFinished):
            if event.status == "error":
                return logging.ERROR
        if isinstance(
            event,
            ProfileStarted
            | BuildDecisionMessage
            | SourceInfoMessage
            | DagStarted
            | DagFinished
            | NodeProgress
            | OperationStarted
            | OperationInfo
            | OperationProgress
            | OperationFinished,
        ):
            return logging.INFO
        if isinstance(event, DagInfo):
            if event.info_name == "source" or event.depth <= 0:
                return logging.INFO
            return logging.DEBUG
        if isinstance(event, NodeStarted | NodeFinished):
            return logging.DEBUG
        raise TypeError(f"Unsupported execution event: {type(event).__name__}")

    @classmethod
    def message(cls, event: ExecutionLogEvent) -> str:
        indent = cls.indent(cls.display_depth(event))
        if isinstance(event, ProfileStarted):
            return (
                f"{indent}Profile: {event.command} {event.name} "
                f"({event.index}/{event.total})"
            )
        if isinstance(
            event,
            ExecutionMessage | BuildDecisionMessage,
        ):
            message = event.message
            if not indent or "\n" not in message:
                return f"{indent}{message}"
            return f"{indent}" + message.replace("\n", f"\n{indent}")
        if isinstance(event, SourceInfoMessage):
            message = f"[{event.source_label}] {event.message}"
            if not indent or "\n" not in message:
                return f"{indent}{message}"
            return f"{indent}" + message.replace("\n", f"\n{indent}")
        if isinstance(event, DagInfo):
            return f"{indent}[{event.dag_name}] {event.info_line}"
        if isinstance(event, OperationInfo):
            return f"{indent}[{event.operation_name}] {event.info_line}"
        if isinstance(event, DagStarted):
            return f"{indent}[{event.dag_name}] started nodes={event.node_count}"
        if isinstance(event, OperationStarted):
            return (
                f"{indent}[{event.operation_name}] started operation={event.entrypoint}"
            )
        if isinstance(event, DagFinished):
            error_suffix = cls.error_suffix(event)
            return (
                f"{indent}[{event.dag_name}] finished "
                f"status={event.status}{error_suffix} items={event.output_items} "
                f"elapsed={event.elapsed_seconds:.6f}s"
            )
        if isinstance(event, OperationFinished):
            error_suffix = cls.error_suffix(event)
            return (
                f"{indent}[{event.operation_name}] finished "
                f"status={event.status}{error_suffix} "
                f"elapsed={event.elapsed_seconds:.6f}s"
            )
        if isinstance(event, NodeStarted):
            calls_suffix = (
                f" calls={event.node_calls_dag}"
                if event.node_calls_dag is not None
                else ""
            )
            label = cls.execution_label(event.dag_name, event.node_name)
            return (
                f"{indent}[{label}] started index={event.node_index} "
                f"execution={event.execution_index} "
                f"kind={event.node_kind}{calls_suffix}"
            )
        if isinstance(event, NodeProgress):
            label = cls.execution_label(event.dag_name, event.node_name)
            return f"{indent}[{label}] {cls.progress_message(event)}"
        if isinstance(event, OperationProgress):
            label = cls.execution_label(event.operation_name, event.step)
            return f"{indent}[{label}] {event.message}"
        if isinstance(event, NodeFinished):
            error_suffix = cls.error_suffix(event)
            label = cls.execution_label(event.dag_name, event.node_name)
            return (
                f"{indent}[{label}] finished index={event.node_index} "
                f"execution={event.execution_index} kind={event.node_kind} "
                f"status={event.status}{error_suffix} items={event.output_items} "
                f"elapsed={event.elapsed_seconds:.6f}s"
            )
        raise TypeError(f"Unsupported execution event: {type(event).__name__}")


def _current_scope() -> ExecutionScope:
    scope = current_execution_scope() or {}
    return ExecutionScope(
        profile_kind=scope.get("profile_kind"),
        profile_name=scope.get("profile_name"),
        target_id=scope.get("target_id"),
        task_id=scope.get("task_id"),
        item_index=scope.get("item_index"),
        item_total=scope.get("item_total"),
    )


def current_source_label(fallback: str) -> str:
    node = current_node_progress_context()
    if node is not None:
        return node.dag_name
    return current_dag_label() or fallback


@contextmanager
def execution_scope(
    *,
    profile_kind: str | None = None,
    profile_name: str | None = None,
    target_id: str | None = None,
    task_id: str | None = None,
    item_index: int | None = None,
    item_total: int | None = None,
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
        if isinstance(event, NodeProgress) and not event.persistent:
            return
        level = ExecutionEventFormatter.level(event)
        if not self._logger.isEnabledFor(level):
            return
        self._logger.log(
            level,
            ExecutionEventFormatter.message(event),
            extra={"dp_event_kind": "execution"},
        )


class ContextExecutionEventSink(ExecutionEventSink):
    """Optional additive sink bound in execution context (used by visuals)."""

    def emit(self, event: ExecutionLogEvent) -> None:
        sink = current_execution_event_sink()
        if sink is None or sink is self:
            return
        sink.emit(event)


def _emit_event(
    event: ExecutionLogEvent,
    logger: logging.Logger | None = None,
) -> None:
    logger_sink = LoggerExecutionEventSink(logger or logging.getLogger(__name__))
    logger_sink.emit(event)
    ContextExecutionEventSink().emit(event)


def emit_execution_message(
    message: str,
    level: int = logging.INFO,
    logger: logging.Logger | None = None,
    depth: int = 0,
) -> None:
    event = ExecutionMessage(
        depth=max(0, int(depth)),
        message=message,
        log_level=int(level),
        scope=_current_scope(),
    )
    _emit_event(event, logger)


def emit_profile_started(
    command: str,
    name: str,
    index: int,
    total: int,
    logger: logging.Logger | None = None,
    depth: int = 0,
) -> None:
    _emit_event(
        ProfileStarted(
            command=command,
            name=name,
            index=index,
            total=total,
            depth=max(0, int(depth)),
            scope=_current_scope(),
        ),
        logger,
    )


def emit_build_decision(
    message: str,
    *,
    logger: logging.Logger | None = None,
    depth: int = 0,
) -> None:
    _emit_event(
        BuildDecisionMessage(
            message=message,
            depth=max(0, int(depth)),
            scope=_current_scope(),
        ),
        logger,
    )


def emit_source_info(
    stream_id: str,
    message: str,
    *,
    logger: logging.Logger | None = None,
    depth: int = 0,
) -> None:
    _emit_event(
        SourceInfoMessage(
            source_label=current_source_label(stream_id),
            message=message,
            depth=max(0, int(depth)),
            scope=_current_scope(),
        ),
        logger,
    )


class ExecutionOperationObserver:
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def emit_operation_event(self, event: ObservedOperationEvent) -> None:
        log_event: _OperationEvent
        if isinstance(event, ObservedOperationStarted):
            log_event = OperationStarted(
                operation_name=event.name,
                depth=event.depth,
                entrypoint=event.entrypoint,
                scope=_current_scope(),
            )
        elif isinstance(event, ObservedOperationInfo):
            log_event = OperationInfo(
                operation_name=event.name,
                depth=event.depth,
                info_line=event.info_line,
                scope=_current_scope(),
            )
        elif isinstance(event, ObservedOperationProgress):
            log_event = OperationProgress(
                operation_name=event.name,
                depth=event.depth,
                step=event.step,
                message=event.message,
                scope=_current_scope(),
            )
        elif isinstance(event, ObservedOperationFinished):
            log_event = OperationFinished(
                operation_name=event.name,
                depth=event.depth,
                status=event.status,
                error_type=event.error_type,
                error_message=event.error_message,
                elapsed_seconds=event.elapsed_seconds,
                scope=_current_scope(),
            )
        else:
            raise TypeError(f"Unsupported operation event: {type(event).__name__}")
        _emit_event(log_event, self._logger)


def make_operation_observer(
    logger: logging.Logger | None = None,
) -> ExecutionOperationObserver:
    return ExecutionOperationObserver(logger or logging.getLogger(__name__))


class HierarchicalExecutionObserver(ExecutionObserver):
    def __init__(self, sink: ExecutionEventSink) -> None:
        self._sink = sink
        set_current_dag_depth(0)
        set_current_dag_label(None)

    def _emit(self, event: ExecutionLogEvent) -> None:
        self._sink.emit(event)

    @staticmethod
    def _metadata_lines(dag_metadata: dict[str, Any] | None) -> list[tuple[str, str]]:
        if not dag_metadata:
            return []
        lines: list[tuple[str, str]] = []
        for key, value in dag_metadata.items():
            if isinstance(value, dict):
                parts = [f"{k}={value[k]}" for k in value]
                lines.append((key, f"{key}: {' '.join(parts)}"))
            else:
                lines.append((key, f"{key}: {value}"))
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
            DagStarted(
                dag_name=dag_name,
                depth=dag_depth,
                node_count=node_count,
                dag_parent=dag_parent,
                scope=_current_scope(),
            )
        )
        for info_name, line in self._metadata_lines(dag_metadata):
            self._emit(
                DagInfo(
                    dag_name=dag_name,
                    depth=dag_depth,
                    info_name=info_name,
                    info_line=line,
                    scope=_current_scope(),
                )
            )
        set_current_dag_depth(dag_depth)
        set_current_dag_label(dag_name)

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
            NodeStarted(
                dag_name=dag_name,
                depth=node_depth,
                node_name=node_name,
                node_index=node_index,
                execution_index=execution_index,
                node_kind=node_kind,
                node_calls_dag=node_calls_dag,
                scope=_current_scope(),
            )
        )
        set_current_dag_depth(node_depth)

    def on_node_end(self, event: NodeExecutionEvent) -> None:
        node_depth = max(0, int(event.depth))
        self._emit(
            NodeFinished(
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
                scope=_current_scope(),
            )
        )
        set_current_dag_depth(node_depth)

    def on_node_progress(self, event: DagNodeProgressEvent) -> None:
        node_depth = max(0, int(event.depth))
        self._emit(
            NodeProgress(
                dag_name=event.dag_name,
                depth=node_depth,
                node_name=event.node_name,
                node_index=event.node_index,
                execution_index=event.execution_index,
                node_kind=event.node_kind,
                node_calls_dag=event.node_calls_dag,
                progress=event.progress,
                elapsed_seconds=event.elapsed_seconds,
                persistent=event.persistent,
                scope=_current_scope(),
            )
        )
        set_current_dag_depth(node_depth)

    def on_dag_end(self, event: DagRunEvent) -> None:
        dag_depth = max(0, int(event.depth))
        self._emit(
            DagFinished(
                dag_name=event.dag_name,
                depth=dag_depth,
                node_count=event.node_count,
                status=event.status,
                error_type=event.error_type,
                error_message=event.error_message,
                output_items=event.output_items,
                elapsed_seconds=event.elapsed_seconds,
                dag_parent=event.parent,
                scope=_current_scope(),
            )
        )
        set_current_dag_depth(dag_depth)
        if event.parent is not None:
            set_current_dag_label(event.parent.dag_name)
        else:
            set_current_dag_label(None)


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
