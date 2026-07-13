import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from datapipeline.artifacts.hydration import hydrate_runtime_artifacts_for_project
from datapipeline.artifacts.planning import ArtifactGraph
from datapipeline.artifacts.validation import validate_artifact_plan
from datapipeline.cli.visuals.execution import emit_execution_message
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.tasks import ArtifactTask
from datapipeline.execution.observability import operation_scope
from datapipeline.operations.dispatch import execute_operation
from datapipeline.operations.persistence import persist_runtime_result
from datapipeline.plugins import RUNTIME_OPERATIONS_EP

from .models import RuntimeJob

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuntimeJobPlan:
    job: RuntimeJob
    required_artifacts: tuple[str, ...]


def validate_build_job(
    task: ArtifactTask,
    graph: ArtifactGraph,
    project_path: Path,
) -> None:
    roots = {task.id}
    artifact_keys = set(graph.dependency_closure(roots))
    if graph.requires_dataset(artifact_keys):
        dataset = load_dataset(project_path)
        artifact_keys = set(graph.active_dependency_closure(roots, dataset))
    validate_artifact_plan(project_path, graph, artifact_keys)


def plan_runtime_job(
    job: RuntimeJob,
    graph: ArtifactGraph,
    project_path: Path,
) -> RuntimeJobPlan:
    feature_dataset = (
        job.dataset if isinstance(job.dataset, FeatureDatasetConfig) else None
    )
    required_artifacts = graph.runtime_dependency_closure(
        job.task,
        preview=job.preview,
        dataset=feature_dataset,
    )
    validate_artifact_plan(project_path, graph, set(required_artifacts))
    return RuntimeJobPlan(job=job, required_artifacts=required_artifacts)


def execute_runtime_job(
    command: Literal["serve", "inspect"],
    project_path: Path,
    graph: ArtifactGraph,
    plan: RuntimeJobPlan,
) -> None:
    job = plan.job
    feature_dataset = (
        job.dataset if isinstance(job.dataset, FeatureDatasetConfig) else None
    )
    current_artifacts = set(
        hydrate_runtime_artifacts_for_project(
            job.runtime,
            project_path,
            graph=graph,
            dataset=feature_dataset,
        )
    )
    unavailable = [
        key for key in plan.required_artifacts if key not in current_artifacts
    ]
    if unavailable:
        logger.error(
            "Runtime task '%s' requires missing or stale artifacts: %s.",
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
                    "task": job.task.model_dump(
                        mode="json",
                        exclude={"version", "kind", "id", "source_path"},
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
        execute_operation(
            operation=job.task,
            operation_group=RUNTIME_OPERATIONS_EP,
            persist=lambda result: persist_runtime_result(
                result,
                target=job.output,
                heartbeat_interval_seconds=job.runtime.heartbeat_interval_seconds,
                logger=logger,
            ),
            operation_task=job.task,
            runtime=job.runtime,
            dataset=job.dataset,
            limit=job.limit,
            target=job.output,
            throttle_ms=job.throttle_ms,
            preview=job.preview,
            visuals=job.observability.visuals,
        )
