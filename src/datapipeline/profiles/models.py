from dataclasses import dataclass, field
from typing import Literal, Sequence

from datapipeline.artifacts.settings import BuildSettings
from datapipeline.config.execution import ExecutionConfig
from datapipeline.config.preview import PreviewStage
from datapipeline.config.tasks import ArtifactTask, OperationTask
from datapipeline.execution.settings import (
    ObservabilitySettings,
)
from datapipeline.io.output import OutputTarget
from datapipeline.io.runs import RunPaths
from datapipeline.runtime import Runtime
from datapipeline.services.definitions import PipelineDefinition


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
    output_ids: tuple[str, ...]


@dataclass(frozen=True)
class MaterializeJob:
    name: str
    stream: str
    output: OutputTarget
    overwrite: bool
    observability: ObservabilitySettings


@dataclass(frozen=True, kw_only=True)
class BuildRunRequest:
    definition: PipelineDefinition
    jobs: Sequence[BuildJob]
    execution: ExecutionConfig
    command: Literal["build"] = field(default="build", init=False)


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
    command: Literal["materialize"] = field(default="materialize", init=False)


ProfileRunRequest = BuildRunRequest | RuntimeRunRequest | MaterializeRunRequest
