from dataclasses import dataclass
from typing import Literal

from datapipeline.dag.node import NodeKind

RunStatus = Literal["success", "error"]


@dataclass(frozen=True)
class DagParentRef:
    dag_name: str
    node_name: str
    node_index: int


@dataclass(frozen=True)
class NodeExecutionEvent:
    dag_name: str
    node_name: str
    node_index: int
    execution_index: int
    output_items: int
    elapsed_seconds: float
    status: RunStatus
    node_kind: NodeKind = "function"
    node_calls_dag: str | None = None
    error_type: str | None = None
    depth: int = 0


@dataclass(frozen=True)
class DagRunEvent:
    dag_name: str
    node_count: int
    output_items: int
    elapsed_seconds: float
    status: RunStatus
    error_type: str | None = None
    depth: int = 0
    parent: DagParentRef | None = None
