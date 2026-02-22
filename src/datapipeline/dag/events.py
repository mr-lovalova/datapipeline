from dataclasses import dataclass
from typing import Literal

RunStatus = Literal["success", "error"]


@dataclass(frozen=True)
class NodeRunEvent:
    dag_name: str
    node_name: str
    stage: int
    output_items: int
    elapsed_seconds: float
    status: RunStatus
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
