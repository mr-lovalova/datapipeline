from dataclasses import dataclass
from typing import Literal

from datapipeline.dag.node import StepKind

RunStatus = Literal["success", "error"]


@dataclass(frozen=True)
class DagParentRef:
    dag_name: str
    step_name: str
    step_index: int


@dataclass(frozen=True)
class StepRunEvent:
    dag_name: str
    step_name: str
    step_index: int
    output_items: int
    elapsed_seconds: float
    status: RunStatus
    step_kind: StepKind = "function"
    step_calls_dag: str | None = None
    error_type: str | None = None
    depth: int = 0


@dataclass(frozen=True)
class DagRunEvent:
    dag_name: str
    step_count: int
    output_items: int
    elapsed_seconds: float
    status: RunStatus
    error_type: str | None = None
    depth: int = 0
    parent: DagParentRef | None = None
