import logging
from typing import Any, Protocol

from datapipeline.dag.events import (
    DagParentRef,
    DagRunEvent,
    NodeExecutionEvent,
    NodeProgressEvent,
)
from datapipeline.dag.node import NodeKind


def _error_suffix(error_type: str | None, error_message: str | None) -> str:
    if error_type is None:
        return ""
    suffix = f" error={error_type}"
    if error_message:
        message = error_message.replace("\n", "\\n")
        suffix = f"{suffix}: {message}"
    return suffix


def _indent(depth: int) -> str:
    return "  " * max(0, int(depth))


def _execution_label(dag_name: str, node_name: str | None = None) -> str:
    if node_name:
        return f"{dag_name}/{node_name}"
    return dag_name


class ExecutionObserver(Protocol):
    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        depth: int = 0,
        dag_metadata: dict[str, Any] | None = None,
        dag_parent: DagParentRef | None = None,
    ) -> None:
        ...

    def on_node_start(
        self,
        *,
        dag_name: str,
        node_name: str,
        node_index: int,
        execution_index: int,
        node_kind: NodeKind = "function",
        node_calls_dag: str | None = None,
        depth: int = 0,
    ) -> None:
        ...

    def on_node_end(self, event: NodeExecutionEvent) -> None:
        ...

    def on_node_progress(self, event: NodeProgressEvent) -> None:
        ...

    def on_dag_end(self, event: DagRunEvent) -> None:
        ...


class NoopExecutionObserver:
    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        depth: int = 0,
        dag_metadata: dict[str, Any] | None = None,
        dag_parent: DagParentRef | None = None,
    ) -> None:
        pass

    def on_node_start(
        self,
        *,
        dag_name: str,
        node_name: str,
        node_index: int,
        execution_index: int,
        node_kind: NodeKind = "function",
        node_calls_dag: str | None = None,
        depth: int = 0,
    ) -> None:
        pass

    def on_node_end(self, event: NodeExecutionEvent) -> None:
        pass

    def on_node_progress(self, event: NodeProgressEvent) -> None:
        pass

    def on_dag_end(self, event: DagRunEvent) -> None:
        pass


class LoggingExecutionObserver:
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        depth: int = 0,
        dag_metadata: dict[str, Any] | None = None,
        dag_parent: DagParentRef | None = None,
    ) -> None:
        if self._logger.isEnabledFor(logging.INFO):
            self._logger.info(
                "%s[%s] started nodes=%d",
                _indent(depth),
                dag_name,
                node_count,
            )

    def on_node_start(
        self,
        *,
        dag_name: str,
        node_name: str,
        node_index: int,
        execution_index: int,
        node_kind: NodeKind = "function",
        node_calls_dag: str | None = None,
        depth: int = 0,
    ) -> None:
        if self._logger.isEnabledFor(logging.DEBUG):
            calls_suffix = f" calls={node_calls_dag}" if node_calls_dag else ""
            self._logger.debug(
                "%s[%s] started index=%d execution=%d kind=%s%s",
                _indent(depth - 1),
                _execution_label(dag_name, node_name),
                node_index,
                execution_index,
                node_kind,
                calls_suffix,
            )

    def on_node_end(self, event: NodeExecutionEvent) -> None:
        if self._logger.isEnabledFor(logging.DEBUG):
            error_suffix = (
                _error_suffix(event.error_type, event.error_message)
                if event.status == "error"
                else ""
            )
            self._logger.debug(
                "%s[%s] finished index=%d execution=%d kind=%s "
                "status=%s%s items=%d elapsed=%.6fs",
                _indent(event.depth - 1),
                _execution_label(event.dag_name, event.node_name),
                event.node_index,
                event.execution_index,
                event.node_kind,
                event.status,
                error_suffix,
                event.output_items,
                event.elapsed_seconds,
            )

    def on_node_progress(self, event: NodeProgressEvent) -> None:
        if self._logger.isEnabledFor(logging.INFO):
            self._logger.info(
                "%s[%s] %s",
                _indent(event.depth - 1),
                _execution_label(event.dag_name, event.node_name),
                event.message,
            )

    def on_dag_end(self, event: DagRunEvent) -> None:
        if self._logger.isEnabledFor(logging.INFO):
            error_suffix = (
                _error_suffix(event.error_type, event.error_message)
                if event.status == "error"
                else ""
            )
            self._logger.info(
                "%s[%s] finished status=%s%s items=%d elapsed=%.6fs",
                _indent(event.depth),
                event.dag_name,
                event.status,
                error_suffix,
                event.output_items,
                event.elapsed_seconds,
            )
