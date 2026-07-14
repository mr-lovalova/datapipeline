import json
import logging
from dataclasses import dataclass
from typing import Literal

from datapipeline.artifacts.hydration import hydrate_runtime_artifacts_for_pipeline
from datapipeline.artifacts.planning import ArtifactGraph
from datapipeline.artifacts.validation import validate_artifact_plan
from datapipeline.cli.visuals.execution import emit_execution_message
from datapipeline.config.tasks import (
    ArtifactTask,
    CoverageTask,
    MatrixTask,
    PipelineTask,
)
from datapipeline.execution.observability import operation_scope
from datapipeline.operations.dispatch import load_operation_runner
from datapipeline.operations.persistence import persist_runtime_result
from datapipeline.operations.runtime.coverage import run_coverage_operation
from datapipeline.operations.runtime.matrix import run_matrix_operation
from datapipeline.operations.runtime.pipeline import run_pipeline_operation
from datapipeline.plugins import RUNTIME_OPERATIONS_EP
from datapipeline.services.definitions import PipelineDefinition

from .models import RuntimeJob

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuntimeJobPlan:
    job: RuntimeJob
    required_artifacts: tuple[str, ...]


def validate_build_job(
    task: ArtifactTask,
    graph: ArtifactGraph,
    definition: PipelineDefinition,
) -> None:
    roots = {task.id}
    artifact_keys = set(graph.dependency_closure(roots))
    if graph.requires_dataset(artifact_keys):
        artifact_keys = set(graph.active_dependency_closure(roots, definition.dataset))
    validate_artifact_plan(definition.streams, graph, artifact_keys)


def plan_runtime_job(
    job: RuntimeJob,
    graph: ArtifactGraph,
    definition: PipelineDefinition,
) -> RuntimeJobPlan:
    if isinstance(job.task, CoverageTask) and job.limit is not None:
        raise ValueError("The coverage operation does not support a record limit.")
    if not isinstance(job.task, PipelineTask) and job.preview is not None:
        raise ValueError("Only the pipeline operation supports preview.")
    if not isinstance(job.task, PipelineTask) and job.throttle_ms is not None:
        raise ValueError("Only the pipeline operation supports throttle_ms.")
    if not isinstance(job.task, PipelineTask) and job.splits:
        raise ValueError("Only the pipeline operation supports split outputs.")
    required_artifacts = graph.runtime_dependency_closure(
        job.task,
        preview=job.preview,
        dataset=definition.dataset,
    )
    validate_artifact_plan(definition.streams, graph, set(required_artifacts))
    return RuntimeJobPlan(job=job, required_artifacts=required_artifacts)


def run_runtime_operation(job: RuntimeJob) -> object:
    task = job.task
    if isinstance(task, PipelineTask):
        return run_pipeline_operation(
            job.runtime,
            job.limit,
            job.output,
            job.throttle_ms,
            job.preview,
        )
    if isinstance(task, MatrixTask):
        return run_matrix_operation(job.runtime, task, job.limit)
    if isinstance(task, CoverageTask):
        return run_coverage_operation(job.runtime, task)

    plugin = load_operation_runner(task, RUNTIME_OPERATIONS_EP)
    return plugin(job.runtime, task, job.limit)


def execute_runtime_job(
    command: Literal["serve", "inspect"],
    definition: PipelineDefinition,
    graph: ArtifactGraph,
    plan: RuntimeJobPlan,
) -> None:
    job = plan.job
    current_artifacts = set(
        hydrate_runtime_artifacts_for_pipeline(
            job.runtime,
            definition,
            graph=graph,
        )
    )
    unavailable = [
        key for key in plan.required_artifacts if key not in current_artifacts
    ]
    if unavailable:
        logger.error(
            "Runtime operation '%s' requires missing or stale artifacts: %s.",
            job.task.id,
            ", ".join(unavailable),
        )
        raise SystemExit(2)

    with operation_scope(f"{command}:{job.name}"):
        emit_execution_message(
            "Config:\n"
            + json.dumps(
                {
                    "target": job.task.id,
                    "operation": job.task.model_dump(
                        mode="json",
                        exclude={"kind", "id"},
                        exclude_none=True,
                    ),
                    "limit": job.limit,
                    "preview": job.preview,
                    "throttle_ms": job.throttle_ms,
                    "splits": list(job.splits),
                    "output": {
                        "transport": job.output.transport,
                        "format": job.output.format,
                        "view": job.output.view,
                        "encoding": job.output.encoding,
                        "destination": (
                            str(job.output.destination)
                            if job.output.destination is not None
                            else None
                        ),
                        "overwrite": job.output.overwrite,
                    },
                    "execution": job.runtime.execution.model_dump(mode="json"),
                    "observability": job.observability.effective_config(),
                },
                indent=2,
            ),
            level=logging.DEBUG,
            logger=logger,
        )
        result = run_runtime_operation(job)
        persist_runtime_result(
            result,
            target=job.output,
            heartbeat_interval_seconds=job.runtime.heartbeat_interval_seconds,
            logger=logger,
        )
