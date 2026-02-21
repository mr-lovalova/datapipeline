import logging
from dataclasses import dataclass
from collections.abc import Sequence
from typing import Any, Literal

from datapipeline.dag.events import DagRunEvent, NodeRunEvent
from datapipeline.dag.observer import ExecutionObserver
from datapipeline.cli.visuals.execution_context import (
    current_execution_event_sink,
    set_current_dag_depth,
)


ExecutionEventKind = Literal[
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
    node_count: int | None = None
    node_name: str | None = None
    stage: int | None = None
    status: str | None = None
    output_items: int | None = None
    elapsed_seconds: float | None = None
    info_line: str | None = None


class ExecutionEventSink:
    def emit(self, event: ExecutionLogEvent) -> None:
        raise NotImplementedError


class ExecutionEventFormatter:
    @staticmethod
    def indent(depth: int) -> str:
        return "  " * max(0, depth)

    @staticmethod
    def level(event: ExecutionLogEvent) -> int:
        if event.kind == "dag_info":
            if event.depth <= 1:
                return logging.INFO
            return logging.DEBUG
        if event.kind in {"node_start", "node_end"}:
            return logging.DEBUG
        if event.depth == 0:
            return logging.INFO
        return logging.DEBUG

    @classmethod
    def message(cls, event: ExecutionLogEvent) -> str:
        indent = cls.indent(event.depth)
        if event.kind == "dag_info":
            return f"{indent}[{event.dag_name}] {event.info_line or ''}"
        if event.kind == "dag_start":
            return (
                f"{indent}DAG started name={event.dag_name} "
                f"nodes={event.node_count}"
            )
        if event.kind == "dag_end":
            return (
                f"{indent}DAG finished name={event.dag_name} "
                f"status={event.status} items={event.output_items} "
                f"elapsed={event.elapsed_seconds:.6f}s"
            )
        if event.kind == "node_start":
            return (
                f"{indent}Node started dag={event.dag_name} "
                f"node={event.node_name} stage={event.stage}"
            )
        return (
            f"{indent}Node finished dag={event.dag_name} "
            f"node={event.node_name} stage={event.stage} "
            f"status={event.status} items={event.output_items} "
            f"elapsed={event.elapsed_seconds:.6f}s"
        )

    @staticmethod
    def extra(event: ExecutionLogEvent) -> dict[str, object]:
        return {
            "dp_event_kind": event.kind,
            "dp_dag_name": event.dag_name,
            "dp_depth": event.depth,
            "dp_node_count": event.node_count,
            "dp_node_name": event.node_name,
            "dp_stage": event.stage,
            "dp_status": event.status,
            "dp_output_items": event.output_items,
            "dp_elapsed_seconds": event.elapsed_seconds,
            "dp_info_line": event.info_line,
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


class ContextOrLoggerExecutionEventSink(ExecutionEventSink):
    def __init__(self, logger_sink: LoggerExecutionEventSink) -> None:
        self._logger_sink = logger_sink

    def emit(self, event: ExecutionLogEvent) -> None:
        context_sink = current_execution_event_sink()
        if context_sink is not None and context_sink is not self:
            context_sink.emit(event)
            return
        self._logger_sink.emit(event)


@dataclass
class _DagFrame:
    name: str
    prefix: str
    depth: int


class HierarchicalExecutionObserver(ExecutionObserver):
    def __init__(self, sink: ExecutionEventSink) -> None:
        self._sink = sink
        self._dag_stack: list[_DagFrame] = []
        set_current_dag_depth(0)

    @staticmethod
    def _prefix(dag_name: str) -> str:
        return dag_name.split(":", 1)[0] if ":" in dag_name else dag_name

    def _active_depth(self) -> int:
        if not self._dag_stack:
            return 0
        return self._dag_stack[-1].depth + 1

    def _find_frame_index(self, dag_name: str) -> int:
        for idx in range(len(self._dag_stack) - 1, -1, -1):
            frame = self._dag_stack[idx]
            if frame.name == dag_name:
                return idx
        return -1

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
        node_count: int,
        dag_metadata: dict[str, Any] | None = None,
    ) -> None:
        if not self._dag_stack:
            depth = 0
        else:
            parent = self._dag_stack[-1]
            if parent.prefix == self._prefix(dag_name):
                depth = parent.depth
            else:
                depth = parent.depth + 1
        self._emit(
            ExecutionLogEvent(
                kind="dag_start",
                dag_name=dag_name,
                depth=depth,
                node_count=node_count,
            )
        )
        for line in self._metadata_lines(dag_metadata):
            self._emit(
                ExecutionLogEvent(
                    kind="dag_info",
                    dag_name=dag_name,
                    depth=depth + 1,
                    info_line=line,
                )
            )
        self._dag_stack.append(
            _DagFrame(
                name=dag_name,
                prefix=self._prefix(dag_name),
                depth=depth,
            )
        )
        set_current_dag_depth(self._active_depth())

    def on_node_start(self, *, dag_name: str, node_name: str, stage: int) -> None:
        depth = self._active_depth()
        self._emit(
            ExecutionLogEvent(
                kind="node_start",
                dag_name=dag_name,
                depth=depth,
                node_name=node_name,
                stage=stage,
            )
        )

    def on_node_end(self, event: NodeRunEvent) -> None:
        depth = self._active_depth()
        self._emit(
            ExecutionLogEvent(
                kind="node_end",
                dag_name=event.dag_name,
                depth=depth,
                node_name=event.node_name,
                stage=event.stage,
                status=event.status,
                output_items=event.output_items,
                elapsed_seconds=event.elapsed_seconds,
            )
        )

    def on_dag_end(self, event: DagRunEvent) -> None:
        idx = self._find_frame_index(event.dag_name)
        depth = 0
        if idx < 0:
            depth = 0
        else:
            depth = self._dag_stack[idx].depth
            self._dag_stack.pop(idx)
        self._emit(
            ExecutionLogEvent(
                kind="dag_end",
                dag_name=event.dag_name,
                depth=depth,
                node_count=event.node_count,
                status=event.status,
                output_items=event.output_items,
                elapsed_seconds=event.elapsed_seconds,
            )
        )
        set_current_dag_depth(self._active_depth())


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
        sink_list = [ContextOrLoggerExecutionEventSink(logger_sink)]

    if not sink_list:
        raise ValueError("'sinks' must contain at least one sink")

    active_sink: ExecutionEventSink
    if len(sink_list) == 1:
        active_sink = sink_list[0]
    else:
        active_sink = CompositeExecutionEventSink(sink_list)
    return HierarchicalExecutionObserver(active_sink)
