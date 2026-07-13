from dataclasses import dataclass
from typing import Literal


RunStatus = Literal["success", "error"]


@dataclass(frozen=True)
class NodeExecutionEvent:
    pipeline_name: str
    node_name: str
    node_index: int
    output_items: int
    elapsed_seconds: float
    status: RunStatus
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class ProgressResource:
    index: int
    total: int
    label: str


@dataclass(frozen=True)
class ProgressSnapshot:
    completed: int
    total: int | None = None
    unit: str = "items"
    phase: str | None = None
    detail: str | None = None
    resource: ProgressResource | None = None


def format_node_progress(
    progress: ProgressSnapshot,
    elapsed_seconds: float,
) -> str:
    parts = [
        "running",
        f"elapsed={elapsed_seconds:.0f}s",
        f"{progress.unit}={progress.completed}",
    ]
    if progress.total is not None:
        parts.append(f"total={progress.total}")
    if progress.phase:
        parts.append(f"phase={progress.phase}")
    if progress.detail:
        parts.append(f"detail={progress.detail}")
    if progress.resource is not None:
        resource = progress.resource
        parts.append(f"resource={resource.index}/{resource.total} {resource.label}")
    return " ".join(parts)


@dataclass(frozen=True)
class NodeProgressEvent:
    pipeline_name: str
    node_name: str
    node_index: int
    progress: ProgressSnapshot
    elapsed_seconds: float
    persistent: bool = False


@dataclass(frozen=True)
class PipelineRunEvent:
    pipeline_name: str
    node_count: int
    output_items: int
    elapsed_seconds: float
    status: RunStatus
    error_type: str | None = None
    error_message: str | None = None
