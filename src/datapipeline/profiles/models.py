from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Sequence, TypeAlias

from datapipeline.config.tasks import ArtifactTask, Task

RuntimeKind = Literal["serve", "inspect"]


@dataclass(frozen=True, kw_only=True)
class BaseExecutionProfile:
    kind: str
    name: str
    target_id: str
    visuals: str
    log_decision: Any
    log_output: Any
    sections: tuple[str, ...] = ()
    label: str | None = None
    artifact_payload: Mapping[str, Any] | None = None
    artifact_writer: Callable[[Mapping[str, Any]], Path | None] | None = None


@dataclass(frozen=True, kw_only=True)
class RuntimeBuildOptions:
    cli_log_level: str | None = None
    cli_visuals: str | None = None
    cli_log_outputs: Sequence[Any] = field(default_factory=tuple)
    workspace: Any = None


@dataclass(frozen=True, kw_only=True)
class RuntimeExecutionProfile(BaseExecutionProfile):
    kind: RuntimeKind
    runtime: Any
    dataset: Any
    limit: int | None = None
    output: Any | None = None
    throttle_ms: float | None = None
    stage: int | None = None
    skip_artifacts: bool = False
    build_options: RuntimeBuildOptions = field(default_factory=RuntimeBuildOptions)


@dataclass(frozen=True, kw_only=True)
class BuildExecutionProfile(BaseExecutionProfile):
    kind: Literal["build"]
    build_settings: Any | None = None


ExecutionProfile: TypeAlias = RuntimeExecutionProfile | BuildExecutionProfile


@dataclass(frozen=True, kw_only=True)
class ProfileRunRequest:
    project_path: Path
    tasks: Sequence[Task]
    artifact_task_configs: Sequence[ArtifactTask]
    profiles: Sequence[ExecutionProfile]


__all__ = [
    "BaseExecutionProfile",
    "BuildExecutionProfile",
    "ExecutionProfile",
    "ProfileRunRequest",
    "RuntimeBuildOptions",
    "RuntimeExecutionProfile",
    "RuntimeKind",
]
