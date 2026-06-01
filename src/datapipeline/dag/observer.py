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
            if dag_parent is None:
                self._logger.info(
                    "DAG started name=%s nodes=%d",
                    dag_name,
                    node_count,
                )
                return
            self._logger.info(
                (
                    "DAG started name=%s nodes=%d "
                    "parent_dag=%s parent_node=%s parent_node_index=%d"
                ),
                dag_name,
                node_count,
                dag_parent.dag_name,
                dag_parent.node_name,
                dag_parent.node_index,
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
                f"Node execution started execution_index=%d dag=%s node=%s index=%d kind=%s{calls_suffix}",
                execution_index,
                dag_name,
                node_name,
                node_index,
                node_kind,
            )

    def on_node_end(self, event: NodeExecutionEvent) -> None:
        if self._logger.isEnabledFor(logging.DEBUG):
            error_suffix = (
                _error_suffix(event.error_type, event.error_message)
                if event.status == "error"
                else ""
            )
            self._logger.debug(
                (
                    "Node execution finished dag=%s node=%s index=%d kind=%s "
                    "execution_index=%d status=%s%s items=%d elapsed=%.6fs"
                ),
                event.dag_name,
                event.node_name,
                event.node_index,
                event.node_kind,
                event.execution_index,
                event.status,
                error_suffix,
                event.output_items,
                event.elapsed_seconds,
            )

    def on_node_progress(self, event: NodeProgressEvent) -> None:
        if self._logger.isEnabledFor(logging.INFO):
            self._logger.info(
                "Node progress dag=%s node=%s index=%d execution_index=%d message=%s",
                event.dag_name,
                event.node_name,
                event.node_index,
                event.execution_index,
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
                "DAG finished name=%s status=%s%s items=%d elapsed=%.6fs",
                event.dag_name,
                event.status,
                error_suffix,
                event.output_items,
                event.elapsed_seconds,
            )
