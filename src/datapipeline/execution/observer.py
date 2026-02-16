from __future__ import annotations

import logging
from typing import Protocol

from datapipeline.execution.run_events import DagRunEvent, NodeRunEvent


class ExecutionObserver(Protocol):
    def on_dag_start(self, *, dag_name: str, node_count: int) -> None:
        ...

    def on_node_start(self, *, dag_name: str, node_name: str, stage: int) -> None:
        ...

    def on_node_end(self, event: NodeRunEvent) -> None:
        ...

    def on_dag_end(self, event: DagRunEvent) -> None:
        ...


class NoopExecutionObserver:
    def on_dag_start(self, *, dag_name: str, node_count: int) -> None:
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

    def on_dag_start(self, *, dag_name: str, node_count: int) -> None:
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(
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
            self._logger.debug(
                "Node finished dag=%s node=%s stage=%d status=%s items=%d elapsed=%.6fs",
                event.dag_name,
                event.node_name,
                event.stage,
                event.status,
                event.output_items,
                event.elapsed_seconds,
            )

    def on_dag_end(self, event: DagRunEvent) -> None:
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(
                "DAG finished name=%s status=%s items=%d elapsed=%.6fs",
                event.dag_name,
                event.status,
                event.output_items,
                event.elapsed_seconds,
            )

