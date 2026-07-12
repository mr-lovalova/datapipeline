import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from datapipeline.cli.visuals.execution_context import (
    current_dag_label,
    current_execution_event_sink,
    set_current_dag_depth,
    set_current_dag_label,
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
    FileResult,
    OperationFinished,
    OperationStarted,
    format_record_count,
)


@dataclass(frozen=True, kw_only=True)
class _ExecutionEvent:
    depth: int = 0


@dataclass(frozen=True, kw_only=True)
class ExecutionMessage(_ExecutionEvent):
    message: str
    log_level: int = logging.INFO


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
class DagSummary(_ExecutionEvent):
    dag_name: str
    summary: str


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
class OperationProgress(_ExecutionEvent):
    operation_name: str
    step: str
    message: str


ExecutionLogEvent = (
    ExecutionMessage
    | FileResult
    | SourceInfoMessage
    | DagStarted
    | DagSummary
    | DagFinished
    | NodeStarted
    | NodeProgress
    | NodeFinished
    | OperationStarted
    | OperationFinished
    | OperationProgress
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
        if isinstance(event, FileResult | OperationStarted | OperationFinished):
            return 0
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
            FileResult
            | SourceInfoMessage
            | DagStarted
            | DagSummary
            | DagFinished
            | NodeProgress
            | OperationStarted
            | OperationFinished
            | OperationProgress,
        ):
            return logging.INFO
        if isinstance(event, NodeStarted | NodeFinished):
            return logging.DEBUG
        raise TypeError(f"Unsupported execution event: {type(event).__name__}")

    @classmethod
    def message(cls, event: ExecutionLogEvent) -> str:
        if isinstance(event, FileResult):
            records = (
                ""
                if event.records is None
                else f" · {format_record_count(event.records)}"
            )
            return f"{event.label}: {event.path}{records}"
        if isinstance(event, OperationStarted):
            return f"Operation {event.name} started"
        if isinstance(event, OperationFinished):
            error_suffix = cls.error_suffix(event)
            return (
                f"Operation {event.name} finished status={event.status}"
                f"{error_suffix} elapsed={event.elapsed_seconds:.6f}s"
            )
        indent = cls.indent(cls.display_depth(event))
        if isinstance(event, ExecutionMessage):
            message = event.message
            if not indent or "\n" not in message:
                return f"{indent}{message}"
            return f"{indent}" + message.replace("\n", f"\n{indent}")
        if isinstance(event, SourceInfoMessage):
            message = f"[{event.source_label}] {event.message}"
            if not indent or "\n" not in message:
                return f"{indent}{message}"
            return f"{indent}" + message.replace("\n", f"\n{indent}")
        if isinstance(event, DagSummary):
            return f"{indent}[{event.dag_name}] {event.summary}"
        if isinstance(event, DagStarted):
            return f"{indent}[{event.dag_name}] started nodes={event.node_count}"
        if isinstance(event, DagFinished):
            error_suffix = cls.error_suffix(event)
            return (
                f"{indent}[{event.dag_name}] finished "
                f"status={event.status}{error_suffix} items={event.output_items} "
                f"elapsed={event.elapsed_seconds:.6f}s"
            )
        if isinstance(event, NodeStarted):
            label = cls.execution_label(event.dag_name, event.node_name)
            return f"{indent}[{label}] started"
        if isinstance(event, NodeProgress):
            label = cls.execution_label(event.dag_name, event.node_name)
            return f"{indent}[{label}] {cls.progress_message(event)}"
        if isinstance(event, OperationProgress):
            return f"Operation {event.operation_name} · {event.step} · {event.message}"
        if isinstance(event, NodeFinished):
            error_suffix = cls.error_suffix(event)
            label = cls.execution_label(event.dag_name, event.node_name)
            return (
                f"{indent}[{label}] finished "
                f"status={event.status}{error_suffix} out={event.output_items} "
                f"elapsed={event.elapsed_seconds:.6f}s"
            )
        raise TypeError(f"Unsupported execution event: {type(event).__name__}")


def current_source_label(fallback: str) -> str:
    node = current_node_progress_context()
    if node is not None:
        return node.dag_name
    return current_dag_label() or fallback


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
    )
    _emit_event(event, logger)


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
        ),
        logger,
    )


class ExecutionOperationObserver:
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def emit_file_result(self, result: FileResult) -> None:
        _emit_event(result, self._logger)

    def emit_started(self, event: OperationStarted) -> None:
        _emit_event(event, self._logger)

    def emit_finished(self, event: OperationFinished) -> None:
        _emit_event(event, self._logger)

    def emit_progress(
        self,
        name: str,
        step: str,
        message: str,
    ) -> None:
        _emit_event(
            OperationProgress(
                operation_name=name,
                step=step,
                message=message,
            ),
            self._logger,
        )


def make_operation_observer(
    logger: logging.Logger | None = None,
) -> ExecutionOperationObserver:
    return ExecutionOperationObserver(logger or logging.getLogger(__name__))


class HierarchicalExecutionObserver(ExecutionObserver):
    def __init__(self, sink: ExecutionEventSink) -> None:
        self._sink = sink
        set_current_dag_depth(0)
        set_current_dag_label(None)

    def on_dag_start(
        self,
        dag_name: str,
        node_count: int,
        depth: int = 0,
        summary: str | None = None,
        dag_parent: DagParentRef | None = None,
    ) -> None:
        dag_depth = max(0, int(depth))
        self._sink.emit(
            DagStarted(
                dag_name=dag_name,
                depth=dag_depth,
                node_count=node_count,
                dag_parent=dag_parent,
            )
        )
        if summary:
            self._sink.emit(
                DagSummary(
                    dag_name=dag_name,
                    depth=dag_depth,
                    summary=summary,
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
        self._sink.emit(
            NodeStarted(
                dag_name=dag_name,
                depth=node_depth,
                node_name=node_name,
                node_index=node_index,
                execution_index=execution_index,
                node_kind=node_kind,
                node_calls_dag=node_calls_dag,
            )
        )
        set_current_dag_depth(node_depth)

    def on_node_end(self, event: NodeExecutionEvent) -> None:
        node_depth = max(0, int(event.depth))
        self._sink.emit(
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
            )
        )
        set_current_dag_depth(node_depth)

    def on_node_progress(self, event: DagNodeProgressEvent) -> None:
        node_depth = max(0, int(event.depth))
        self._sink.emit(
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
            )
        )
        set_current_dag_depth(node_depth)

    def on_dag_end(self, event: DagRunEvent) -> None:
        dag_depth = max(0, int(event.depth))
        self._sink.emit(
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
