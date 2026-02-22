import logging
from typing import Any, Protocol

from datapipeline.dag.events import DagRunEvent, NodeRunEvent


class ExecutionObserver(Protocol):
    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        dag_metadata: dict[str, Any] | None = None,
    ) -> None:
        ...

    def on_node_start(self, *, dag_name: str, node_name: str, stage: int) -> None:
        ...

    def on_node_end(self, event: NodeRunEvent) -> None:
        ...

    def on_dag_end(self, event: DagRunEvent) -> None:
        ...


class NoopExecutionObserver:
    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        dag_metadata: dict[str, Any] | None = None,
    ) -> None:
        pass

    def on_node_start(self, *, dag_name: str, node_name: str, stage: int) -> None:
        pass

    def on_node_end(self, event: NodeRunEvent) -> None:
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
        dag_metadata: dict[str, Any] | None = None,
    ) -> None:
        if self._logger.isEnabledFor(logging.INFO):
            self._logger.info(
                "DAG started name=%s nodes=%d",
                dag_name,
                node_count,
            )

    def on_node_start(self, *, dag_name: str, node_name: str, stage: int) -> None:
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(
                "Node started dag=%s node=%s stage=%d",
                dag_name,
                node_name,
                stage,
            )

    def on_node_end(self, event: NodeRunEvent) -> None:
        if self._logger.isEnabledFor(logging.DEBUG):
            error_suffix = (
                f" error={event.error_type}"
                if event.status == "error" and event.error_type
                else ""
            )
            self._logger.debug(
                "Node finished dag=%s node=%s stage=%d status=%s%s items=%d elapsed=%.6fs",
                event.dag_name,
                event.node_name,
                event.stage,
                event.status,
                error_suffix,
                event.output_items,
                event.elapsed_seconds,
            )

    def on_dag_end(self, event: DagRunEvent) -> None:
        if self._logger.isEnabledFor(logging.INFO):
            error_suffix = (
                f" error={event.error_type}"
                if event.status == "error" and event.error_type
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
