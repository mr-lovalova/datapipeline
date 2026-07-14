from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Sequence

from datapipeline.artifacts.settings import BuildSettings
from datapipeline.config.execution import ExecutionConfig
from datapipeline.config.preview import PreviewStage
from datapipeline.execution.settings import (
    ObservabilitySettings,
)
from datapipeline.config.tasks import ArtifactTask, OperationTask
from datapipeline.io.output import OutputTarget
from datapipeline.runtime import Runtime
from datapipeline.services.definitions import PipelineDefinition
from datapipeline.services.runs import RunPaths

TaskProfileKind = Literal["serve", "build", "inspect"]


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
    output: OutputTarget
    observability: ObservabilitySettings
    limit: int | None
    throttle_ms: float | None
    preview: PreviewStage | None
    output_splits: tuple[str, ...]


@dataclass(frozen=True)
class MaterializeJob:
    name: str
    stream: str
    output: Path
    overwrite: bool
    observability: ObservabilitySettings


@dataclass(frozen=True, kw_only=True)
class BuildRunRequest:
    definition: PipelineDefinition
    jobs: Sequence[BuildJob]
    execution: ExecutionConfig


@dataclass(frozen=True, kw_only=True)
class RuntimeRunRequest:
    command: Literal["serve", "inspect"]
    definition: PipelineDefinition
    jobs: Sequence[RuntimeJob]
    execution: ExecutionConfig
    artifact_settings: BuildSettings
    serve_run_plans: tuple[ServeRunPlan, ...] = ()


@dataclass(frozen=True, kw_only=True)
class MaterializeRunRequest:
    definition: PipelineDefinition
    jobs: Sequence[MaterializeJob]
    execution: ExecutionConfig
    artifact_settings: BuildSettings
    runtime: Runtime


ProfileRunRequest = BuildRunRequest | RuntimeRunRequest | MaterializeRunRequest
