from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

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

ProfileKind = Literal["serve", "build", "inspect"]
ProfileDataset = FeatureDatasetConfig | RecordDatasetConfig


@dataclass(frozen=True, kw_only=True)
class ExecutionProfile:
    name: str
    target_id: str
    visuals: str
    log_decision: LogLevelDecision
    log_output: LogOutputSettings
    sections: tuple[str, ...] = ()
    label: str | None = None
    profile_report: Mapping[str, Any] | None = None
    runtime: Runtime | None = None
    dataset: ProfileDataset | None = None
    limit: int | None = None
    output: OutputTarget | None = None
    throttle_ms: float | None = None
    stage: int | None = None
    build_mode: str | None = None
    build_settings: BuildSettings | None = None

@dataclass(frozen=True, kw_only=True)
class ProfileRunRequest:
    command: ProfileKind
    project_path: Path
    tasks: Sequence[Task]
    artifact_task_configs: Sequence[ArtifactTask]
    profiles: Sequence[ExecutionProfile]
    skip_build: bool = False
    cli_log_level: str | None = None
    cli_visuals: str | None = None
    cli_log_outputs: Sequence[LogOutputTarget] = field(default_factory=tuple)
    workspace: WorkspaceContext | None = None


__all__ = [
    "ExecutionProfile",
    "ProfileKind",
    "ProfileDataset",
    "ProfileRunRequest",
]
