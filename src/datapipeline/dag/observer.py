from typing import Protocol

from datapipeline.dag.events import (
    DagParentRef,
    DagRunEvent,
    NodeExecutionEvent,
    NodeProgressEvent,
)
from datapipeline.dag.node import NodeKind


class ExecutionObserver(Protocol):
    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        depth: int = 0,
        summary: str | None = None,
        dag_parent: DagParentRef | None = None,
    ) -> None: ...

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
    ) -> None: ...

    def on_node_end(self, event: NodeExecutionEvent) -> None: ...

    def on_node_progress(self, event: NodeProgressEvent) -> None: ...

    def on_dag_end(self, event: DagRunEvent) -> None: ...


class NoopExecutionObserver:
    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        depth: int = 0,
        summary: str | None = None,
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
