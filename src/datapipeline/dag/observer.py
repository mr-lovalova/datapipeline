import logging
from typing import Any, Protocol

from datapipeline.dag.events import DagParentRef, DagRunEvent, StepRunEvent
from datapipeline.dag.node import StepKind


class ExecutionObserver(Protocol):
    def on_dag_start(
        self,
        *,
        dag_name: str,
        step_count: int,
        depth: int = 0,
        dag_metadata: dict[str, Any] | None = None,
        dag_parent: DagParentRef | None = None,
    ) -> None:
        ...

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
        ...

    def on_step_end(self, event: StepRunEvent) -> None:
        ...

    def on_dag_end(self, event: DagRunEvent) -> None:
        ...


class NoopExecutionObserver:
    def on_dag_start(
        self,
        *,
        dag_name: str,
        step_count: int,
        depth: int = 0,
        dag_metadata: dict[str, Any] | None = None,
        dag_parent: DagParentRef | None = None,
    ) -> None:
        pass

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
        pass

    def on_step_end(self, event: StepRunEvent) -> None:
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
        step_count: int,
        depth: int = 0,
        dag_metadata: dict[str, Any] | None = None,
        dag_parent: DagParentRef | None = None,
    ) -> None:
        if self._logger.isEnabledFor(logging.INFO):
            if dag_parent is None:
                self._logger.info(
                    "DAG started name=%s steps=%d",
                    dag_name,
                    step_count,
                )
                return
            self._logger.info(
                (
                    "DAG started name=%s steps=%d "
                    "parent_dag=%s parent_step=%s parent_step_index=%d"
                ),
                dag_name,
                step_count,
                dag_parent.dag_name,
                dag_parent.step_name,
                dag_parent.step_index,
            )

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
        if self._logger.isEnabledFor(logging.DEBUG):
            calls_suffix = f" calls={step_calls_dag}" if step_calls_dag else ""
            self._logger.debug(
                f"Step activated dag=%s step=%s index=%d kind=%s{calls_suffix}",
                dag_name,
                step_name,
                step_index,
                step_kind,
            )

    def on_step_end(self, event: StepRunEvent) -> None:
        if self._logger.isEnabledFor(logging.DEBUG):
            error_suffix = (
                f" error={event.error_type}"
                if event.status == "error" and event.error_type
                else ""
            )
            self._logger.debug(
                (
                    "Step finished dag=%s step=%s index=%d kind=%s "
                    "status=%s%s items=%d elapsed=%.6fs"
                ),
                event.dag_name,
                event.step_name,
                event.step_index,
                event.step_kind,
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
