import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from datapipeline.artifacts.hydration import hydrate_runtime_artifacts
from datapipeline.artifacts.planning import ArtifactGraph
from datapipeline.artifacts.validation import validate_artifact_plan
from datapipeline.build.state import (
    BuildState,
    load_build_state,
    save_build_state,
)
from datapipeline.build.config_hash import compute_config_hash
from datapipeline.cli.visuals.execution import emit_build_decision, emit_execution_message
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.tasks import ArtifactTask
from datapipeline.execution.observability import operation_scope
from datapipeline.operations.dispatch import execute_operation
from datapipeline.operations.persistence import ArtifactOutput, persist_artifact_output
from datapipeline.plugins import BUILD_OPERATIONS_EP
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import (
    artifacts_root,
    build_state_path,
)
from datapipeline.services.project_paths import tasks_dir

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArtifactBuildJob:
    task: ArtifactTask
    invalidated_artifacts: tuple[str, ...]


@dataclass(frozen=True)
class SkippedBuild:
    reason: Literal[
        "already_resolved",
        "mode_off",
        "no_artifacts_selected",
        "not_required",
        "up_to_date",
    ]
    artifacts: tuple[str, ...]


@dataclass(frozen=True)
class BuildPlan:
    reason: Literal["force", "missing", "stale"]
    artifacts: tuple[str, ...]
    jobs: tuple[ArtifactBuildJob, ...]
    skipped_current: tuple[str, ...]
    config_hash: str
    state_path: Path
    previous_state: BuildState | None
    graph: ArtifactGraph


BuildDecision = BuildPlan | SkippedBuild


def _log_build_decision(
    plan: BuildDecision,
    *,
    mode: str,
    profile_name: str | None,
) -> None:
    action = "run" if isinstance(plan, BuildPlan) else "skip"
    payload: dict[str, object] = {
        "action": action,
        "reason": plan.reason,
        "mode": mode,
    }
    if profile_name is not None:
        payload["profile"] = profile_name
    payload["selected_artifacts"] = len(plan.artifacts)
    payload["expanded_artifacts"] = list(plan.artifacts)
    if isinstance(plan, BuildPlan):
        payload["jobs"] = [job.task.id for job in plan.jobs]
        skipped_current = plan.skipped_current
    else:
        payload["jobs"] = []
        skipped_current = plan.artifacts
    if skipped_current:
        payload["skipped_current"] = list(skipped_current)
    emit_build_decision(
        f"Build decision:\n{json.dumps(payload, indent=2, default=str)}",
        logger=logger,
    )


def _run_artifact_builder(
    runtime: Runtime,
    task: ArtifactTask,
) -> ArtifactOutput | None:
    with operation_scope(f"build:{task.id}"):
        return execute_operation(
            operation=task,
            operation_group=BUILD_OPERATIONS_EP,
            persist=lambda result: persist_artifact_output(
                result,
                artifact_key=task.id,
                expected_relative_path=task.output,
                runtime=runtime,
            ),
            runtime=runtime,
            task_cfg=task,
        )


def _plan_build(
    *,
    project_path: Path,
    graph: ArtifactGraph,
    required_artifacts: set[str],
    mode: str,
    resolved_artifacts: set[str] | None = None,
    expected_config_hash: str | None = None,
) -> BuildDecision:
    try:
        selected_roots = set(required_artifacts)
        selected_keys = set(graph.dependency_closure(selected_roots))
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    if not selected_keys:
        return SkippedBuild(reason="no_artifacts_selected", artifacts=())

    config_hash = compute_config_hash(project_path, tasks_dir(project_path))
    if expected_config_hash is not None and config_hash != expected_config_hash:
        raise RuntimeError(
            "Build inputs changed after profiles were resolved; rerun the build."
        )

    dataset = None
    if graph.requires_dataset(selected_keys):
        dataset = load_dataset(project_path, "vectors")
        selected_keys = set(graph.active_dependency_closure(selected_roots, dataset))
    if not selected_keys:
        return SkippedBuild(reason="not_required", artifacts=())

    expanded_artifacts = graph.topological_order(selected_keys)
    try:
        validate_artifact_plan(project_path, graph, selected_keys)
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    state_path = build_state_path(project_path)
    previous_state = load_build_state(state_path)
    resolved = resolved_artifacts if resolved_artifacts is not None else set()
    freshness = graph.freshness(
        keys=selected_keys | resolved,
        state=previous_state,
        config_hash=config_hash,
        artifacts_root=artifacts_root(project_path),
    )
    stale_resolved = freshness.outdated & resolved
    if stale_resolved:
        artifacts = ", ".join(graph.topological_order(stale_resolved))
        raise RuntimeError(
            "Artifacts resolved by an earlier build profile became stale before "
            f"the command completed: {artifacts}. Rerun the build."
        )
    selected_outdated = freshness.outdated & selected_keys
    if mode == "OFF":
        if selected_outdated:
            artifacts = ", ".join(graph.topological_order(selected_outdated))
            logger.error(
                "Artifact mode is OFF, but required artifacts are missing or stale: %s.",
                artifacts,
            )
            raise SystemExit(2)
        return SkippedBuild(
            reason="mode_off",
            artifacts=expanded_artifacts,
        )

    force = mode == "FORCE"
    if not force and not selected_outdated:
        return SkippedBuild(
            reason="up_to_date",
            artifacts=expanded_artifacts,
        )

    build_keys = set(selected_keys) - resolved if force else set(selected_outdated)
    if not build_keys:
        return SkippedBuild(
            reason="already_resolved",
            artifacts=expanded_artifacts,
        )
    skipped_current = tuple(key for key in expanded_artifacts if key not in build_keys)
    all_active_keys = (
        set(
            graph.active_dependency_closure(
                (definition.key for definition in graph.definitions),
                dataset,
            )
        )
        if dataset is not None
        else {definition.key for definition in graph.definitions}
    )
    jobs = tuple(
        ArtifactBuildJob(
            task=graph.tasks_by_id[key],
            invalidated_artifacts=graph.topological_order(
                {key}
                | graph.dependents_of(
                    {key},
                    active_keys=all_active_keys,
                )
            ),
        )
        for key in graph.topological_order(build_keys)
    )
    return BuildPlan(
        reason=(
            "force"
            if force
            else ("missing" if freshness.missing & selected_keys else "stale")
        ),
        artifacts=expanded_artifacts,
        jobs=jobs,
        skipped_current=skipped_current,
        config_hash=config_hash,
        state_path=state_path,
        previous_state=previous_state,
        graph=graph,
    )


def _execute_build_jobs(
    *,
    runtime: Runtime,
    plan: BuildPlan,
) -> BuildState:
    current_state = _merge_build_state(
        previous_state=plan.previous_state,
        built_artifacts={},
        config_hash=plan.config_hash,
    )
    for job in plan.jobs:
        removed = False
        for key in job.invalidated_artifacts:
            if current_state.artifacts.pop(key, None) is not None:
                removed = True
        if removed:
            save_build_state(current_state, plan.state_path)
        hydrate_runtime_artifacts(
            runtime=runtime,
            graph=plan.graph,
            state=current_state,
            config_hash=plan.config_hash,
            artifact_keys=plan.artifacts,
        )

        result = _run_artifact_builder(
            runtime=runtime,
            task=job.task,
        )
        if result is None:
            raise RuntimeError(f"Artifact task '{job.task.id}' produced no artifact.")
        current_state = _merge_build_state(
            previous_state=current_state,
            built_artifacts={job.task.id: result},
            config_hash=plan.config_hash,
        )
        save_build_state(current_state, plan.state_path)
        hydrate_runtime_artifacts(
            runtime=runtime,
            graph=plan.graph,
            state=current_state,
            config_hash=plan.config_hash,
            artifact_keys=plan.artifacts,
        )
        label = job.task.id.replace("_", " ").capitalize()
        path = (Path(runtime.artifacts_root) / result.relative_path).resolve()
        emit_execution_message(f"{label}: {path}", logger=logger)
    return current_state


def _merge_build_state(
    previous_state: BuildState | None,
    built_artifacts: dict[str, ArtifactOutput],
    config_hash: str,
) -> BuildState:
    new_state = BuildState()
    if previous_state is not None:
        new_state.artifacts.update(previous_state.artifacts)
    for key, output in built_artifacts.items():
        new_state.register(
            key,
            output.relative_path,
            config_hash=config_hash,
            meta=output.meta,
        )
    return new_state


def run_build_if_needed(
    project: Path | str,
    *,
    graph: ArtifactGraph,
    required_artifacts: set[str],
    mode: str,
    runtime: Runtime,
    profile_name: str | None = None,
    heartbeat_interval_seconds: float | None = None,
    resolved_artifacts: set[str] | None = None,
    expected_config_hash: str | None = None,
) -> bool:
    """Execute artifact-producing operations when selected artifacts are missing or stale."""
    project_path = Path(project).resolve()
    mode = mode.upper()
    if mode not in {"AUTO", "FORCE", "OFF"}:
        raise ValueError(f"Unknown artifact mode '{mode}'.")

    plan = _plan_build(
        project_path=project_path,
        graph=graph,
        required_artifacts=required_artifacts,
        mode=mode,
        resolved_artifacts=resolved_artifacts,
        expected_config_hash=expected_config_hash,
    )
    _log_build_decision(
        plan,
        mode=mode,
        profile_name=profile_name,
    )
    if isinstance(plan, SkippedBuild):
        if resolved_artifacts is not None:
            resolved_artifacts.update(plan.artifacts)
        return False

    if heartbeat_interval_seconds is not None:
        runtime.heartbeat_interval_seconds = heartbeat_interval_seconds
    _execute_build_jobs(
        runtime=runtime,
        plan=plan,
    )
    if compute_config_hash(project_path, tasks_dir(project_path)) != plan.config_hash:
        raise RuntimeError(
            "Build inputs changed while artifacts were being generated; rerun the build."
        )
    if resolved_artifacts is not None:
        resolved_artifacts.update(plan.artifacts)
    return True
