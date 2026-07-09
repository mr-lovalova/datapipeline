from dataclasses import dataclass
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
)
from datapipeline.config.tasks import ArtifactTask, Task
from datapipeline.io.output import OutputTarget
from datapipeline.runtime import Runtime
from datapipeline.services.executions import ExecutionPaths
from datapipeline.services.runs import RunPaths

ProfileKind = Literal["serve", "build", "inspect"]
ProfileDataset = FeatureDatasetConfig | RecordDatasetConfig


@dataclass(frozen=True)
class ServeRunPlan:
    paths: RunPaths
    preview_index: int | None


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
    preview_index: int | None = None
    build_mode: str | None = None
    build_settings: BuildSettings | None = None
    heartbeat_interval_seconds: float | None = None


@dataclass(frozen=True, kw_only=True)
class ProfileRunRequest:
    command: ProfileKind
    project_path: Path
    execution: ExecutionPaths
    tasks: Sequence[Task]
    artifact_task_configs: Sequence[ArtifactTask]
    profiles: Sequence[ExecutionProfile]
    serve_run_plans: tuple[ServeRunPlan, ...] = ()
    skip_build: bool = False


__all__ = [
    "ExecutionProfile",
    "ServeRunPlan",
    "ProfileKind",
    "ProfileDataset",
    "ProfileRunRequest",
]
