from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Sequence

from datapipeline.config.build_resolution import BuildSettings
from datapipeline.config.execution import ExecutionConfig
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.preview import PreviewStage
from datapipeline.config.resolution import (
    ObservabilitySettings,
)
from datapipeline.config.tasks import ArtifactTask, OperationTask
from datapipeline.io.output import OutputTarget
from datapipeline.runtime import Runtime
from datapipeline.services.runs import RunPaths

ProfileKind = Literal["serve", "build", "inspect"]


@dataclass(frozen=True)
class ServeRunPlan:
    paths: RunPaths
    preview: PreviewStage | None


@dataclass(frozen=True)
class BuildJob:
    task: ArtifactTask
    settings: BuildSettings


@dataclass(frozen=True)
class RuntimeJob:
    name: str
    task: OperationTask
    runtime: Runtime
    dataset: FeatureDatasetConfig
    output: OutputTarget
    observability: ObservabilitySettings
    limit: int | None
    throttle_ms: float | None
    preview: PreviewStage | None
    splits: tuple[str, ...]


@dataclass(frozen=True, kw_only=True)
class BuildRunRequest:
    project_path: Path
    artifact_task_configs: Sequence[ArtifactTask]
    jobs: Sequence[BuildJob]
    execution: ExecutionConfig
    config_hash: str


@dataclass(frozen=True, kw_only=True)
class RuntimeRunRequest:
    command: Literal["serve", "inspect"]
    project_path: Path
    artifact_task_configs: Sequence[ArtifactTask]
    jobs: Sequence[RuntimeJob]
    execution: ExecutionConfig
    config_hash: str
    artifact_settings: BuildSettings
    serve_run_plans: tuple[ServeRunPlan, ...] = ()


ProfileRunRequest = BuildRunRequest | RuntimeRunRequest
