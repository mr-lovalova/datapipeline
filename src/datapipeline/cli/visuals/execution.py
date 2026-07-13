import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from datapipeline.cli.visuals.execution_context import (
    current_execution_event_sink,
)
from datapipeline.execution.events import (
    PipelineRunEvent,
    format_node_progress,
    NodeExecutionEvent,
    NodeProgressEvent as PipelineNodeProgressEvent,
    ProgressSnapshot,
    RunStatus,
)
from datapipeline.execution.observer import PipelineObserver
from datapipeline.execution.observability import (
    FileResult,
    OperationFinished,
    OperationStarted,
)


@dataclass(frozen=True, kw_only=True)
class ExecutionMessage:
    message: str
    log_level: int = logging.INFO


@dataclass(frozen=True, kw_only=True)
class PipelineStarted:
    pipeline_name: str
    node_count: int


@dataclass(frozen=True, kw_only=True)
class PipelineSummary:
    pipeline_name: str
    summary: str


@dataclass(frozen=True, kw_only=True)
class PipelineFinished:
    pipeline_name: str
    node_count: int
    status: RunStatus
    output_items: int
    elapsed_seconds: float
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, kw_only=True)
class _NodeEvent:
    pipeline_name: str
    node_name: str
    node_index: int


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
class OperationProgress:
    operation_name: str
    step: str
    message: str


ExecutionLogEvent = (
    ExecutionMessage
    | FileResult
    | PipelineStarted
    | PipelineSummary
    | PipelineFinished
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
        event: PipelineFinished | NodeFinished | OperationFinished,
    ) -> str:
        if event.status != "error" or event.error_type is None:
            return ""
        suffix = f" error={event.error_type}"
        if event.error_message:
            message = event.error_message.replace("\n", "\\n")
            suffix = f"{suffix}: {message}"
        return suffix

    @staticmethod
    def execution_label(pipeline_name: str, node_name: str | None) -> str:
        if node_name:
            return f"{pipeline_name}/{node_name}"
        return pipeline_name

    @staticmethod
    def progress_message(event: NodeProgress) -> str:
        return format_node_progress(event.progress, event.elapsed_seconds)

    @staticmethod
    def level(event: ExecutionLogEvent) -> int:
        if isinstance(event, ExecutionMessage):
            return int(event.log_level)
        if isinstance(event, PipelineFinished | NodeFinished | OperationFinished):
            if event.status == "error":
                return logging.ERROR
        if isinstance(
            event,
            FileResult
            | PipelineStarted
            | PipelineSummary
            | PipelineFinished
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
            return f"{event.label}: {event.path}"
        if isinstance(event, OperationStarted):
            return f"Operation {event.name} started"
        if isinstance(event, OperationFinished):
            error_suffix = cls.error_suffix(event)
            return (
                f"Operation {event.name} finished status={event.status}"
                f"{error_suffix} elapsed={event.elapsed_seconds:.6f}s"
            )
        if isinstance(event, ExecutionMessage):
            return event.message
        if isinstance(event, PipelineSummary):
            return f"[{event.pipeline_name}] {event.summary}"
        if isinstance(event, PipelineStarted):
            return f"[{event.pipeline_name}] started nodes={event.node_count}"
        if isinstance(event, PipelineFinished):
            error_suffix = cls.error_suffix(event)
            return (
                f"[{event.pipeline_name}] finished "
                f"status={event.status}{error_suffix} items={event.output_items} "
                f"elapsed={event.elapsed_seconds:.6f}s"
            )
        if isinstance(event, NodeStarted):
            label = cls.execution_label(event.pipeline_name, event.node_name)
            return f"[{label}] started"
        if isinstance(event, NodeProgress):
            label = cls.execution_label(event.pipeline_name, event.node_name)
            return f"[{label}] {cls.progress_message(event)}"
        if isinstance(event, OperationProgress):
            return f"Operation {event.operation_name} · {event.step} · {event.message}"
        if isinstance(event, NodeFinished):
            error_suffix = cls.error_suffix(event)
            label = cls.execution_label(event.pipeline_name, event.node_name)
            return (
                f"[{label}] finished "
                f"status={event.status}{error_suffix} out={event.output_items} "
                f"elapsed={event.elapsed_seconds:.6f}s"
            )
        raise TypeError(f"Unsupported execution event: {type(event).__name__}")


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
) -> None:
    event = ExecutionMessage(
        message=message,
        log_level=int(level),
    )
    _emit_event(event, logger)


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


class PipelineEventObserver(PipelineObserver):
    def __init__(self, sink: ExecutionEventSink) -> None:
        self._sink = sink

    def on_pipeline_start(
        self,
        pipeline_name: str,
        node_count: int,
        summary: str | None = None,
    ) -> None:
        self._sink.emit(
            PipelineStarted(
                pipeline_name=pipeline_name,
                node_count=node_count,
            )
        )
        if summary:
            self._sink.emit(
                PipelineSummary(
                    pipeline_name=pipeline_name,
                    summary=summary,
                )
            )

    def on_node_start(
        self,
        pipeline_name: str,
        node_name: str,
        node_index: int,
    ) -> None:
        self._sink.emit(
            NodeStarted(
                pipeline_name=pipeline_name,
                node_name=node_name,
                node_index=node_index,
            )
        )

    def on_node_end(self, event: NodeExecutionEvent) -> None:
        self._sink.emit(
            NodeFinished(
                pipeline_name=event.pipeline_name,
                node_name=event.node_name,
                node_index=event.node_index,
                status=event.status,
                error_type=event.error_type,
                error_message=event.error_message,
                output_items=event.output_items,
                elapsed_seconds=event.elapsed_seconds,
            )
        )

    def on_node_progress(self, event: PipelineNodeProgressEvent) -> None:
        self._sink.emit(
            NodeProgress(
                pipeline_name=event.pipeline_name,
                node_name=event.node_name,
                node_index=event.node_index,
                progress=event.progress,
                elapsed_seconds=event.elapsed_seconds,
                persistent=event.persistent,
            )
        )

    def on_pipeline_end(self, event: PipelineRunEvent) -> None:
        self._sink.emit(
            PipelineFinished(
                pipeline_name=event.pipeline_name,
                node_count=event.node_count,
                status=event.status,
                error_type=event.error_type,
                error_message=event.error_message,
                output_items=event.output_items,
                elapsed_seconds=event.elapsed_seconds,
            )
        )


def make_pipeline_observer(
    logger: logging.Logger | None = None,
    sink: ExecutionEventSink | None = None,
    sinks: Sequence[ExecutionEventSink] | None = None,
) -> PipelineObserver:
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
    return PipelineEventObserver(active_sink)
