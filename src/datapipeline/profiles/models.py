from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Sequence, TypeAlias

from datapipeline.config.build_resolution import BuildSettings
from datapipeline.config.dataset.dataset import (
    FeatureDatasetConfig,
    RecordDatasetConfig,
)
from datapipeline.config.resolution import (
    LogLevelDecision,
    LogOutputSettings,
    LogOutputTarget,
)
from datapipeline.config.workspace import WorkspaceContext
from datapipeline.config.tasks import ArtifactTask, Task
from datapipeline.io.output import OutputTarget
from datapipeline.runtime import Runtime

RuntimeKind = Literal["serve", "inspect"]
ProfileDataset = FeatureDatasetConfig | RecordDatasetConfig


@dataclass(frozen=True, kw_only=True)
class BaseExecutionProfile:
    kind: str
    name: str
    target_id: str
    visuals: str
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    sections: tuple[str, ...] = ()
    label: str | None = None
    artifact_payload: Mapping[str, Any] | None = None
    artifact_writer: Callable[[Mapping[str, Any]], Path | None] | None = None


@dataclass(frozen=True, kw_only=True)
class RuntimeBuildOptions:
    build_mode: str | None = None
    cli_log_level: str | None = None
    cli_visuals: str | None = None
    cli_log_outputs: Sequence[LogOutputTarget] = field(default_factory=tuple)
    workspace: WorkspaceContext | None = None


@dataclass(frozen=True, kw_only=True)
class RuntimeExecutionProfile(BaseExecutionProfile):
    kind: RuntimeKind
    runtime: Runtime
    dataset: ProfileDataset
    limit: int | None = None
    output: OutputTarget | None = None
    throttle_ms: float | None = None
    stage: int | None = None
    skip_artifacts: bool = False
    build_options: RuntimeBuildOptions = field(default_factory=RuntimeBuildOptions)


@dataclass(frozen=True, kw_only=True)
class BuildExecutionProfile(BaseExecutionProfile):
    kind: Literal["build"]
    build_settings: BuildSettings | None = None


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
    "ProfileDataset",
    "ProfileRunRequest",
    "RuntimeBuildOptions",
    "RuntimeExecutionProfile",
    "RuntimeKind",
]
