import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from datapipeline.artifacts.hydration import hydrate_runtime_artifacts
from datapipeline.artifacts.planning import ArtifactGraph, build_artifact_graph
from datapipeline.artifacts.specs import ArtifactDefinition
from datapipeline.artifacts.validation import validate_artifact_plan
from datapipeline.build.state import (
    ArtifactInfo,
    BuildState,
    load_build_state,
    save_build_state,
)
from datapipeline.build.config_hash import compute_config_hash
from datapipeline.cli.logging_setup import configure_root_logging
from datapipeline.cli.visuals.execution import (
    emit_build_decision,
    make_execution_observer,
    make_operation_observer,
)
from datapipeline.config.build_resolution import BuildSettings, resolve_build_settings
from datapipeline.config.dataset.loader import load_dataset
from datapipeline.config.profiles import BuildProfile
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.loaders.operations import operation_specs
from datapipeline.config.tasks import ArtifactTask
from datapipeline.execution.observability import operation_observer, operation_scope
from datapipeline.operations.dispatch import execute_operation
from datapipeline.operations.persistence import ArtifactOutput, persist_artifact_output
from datapipeline.plugins import BUILD_OPERATIONS_EP
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import (
    artifacts_root,
    bootstrap_build_runtime,
    build_state_path,
)
from datapipeline.services.project_paths import tasks_dir

logger = logging.getLogger(__name__)
_ARTIFACT_CONFIG_HASH_META_KEY = "_config_hash"


@dataclass(frozen=True)
class ArtifactBuildJob:
    definition: ArtifactDefinition
    task: ArtifactTask
    invalidated_artifacts: tuple[str, ...]


@dataclass(frozen=True)
class SkippedBuild:
    reason: Literal[
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
    settings: BuildSettings,
) -> None:
    action = "run" if isinstance(plan, BuildPlan) else "skip"
    payload: dict[str, object] = {
        "action": action,
        "reason": plan.reason,
        "mode": settings.mode,
        "profile": settings.profile_name,
    }
    payload["selected_artifacts"] = len(plan.artifacts)
    payload["expanded_artifacts"] = list(plan.artifacts)
    if isinstance(plan, BuildPlan):
        payload["jobs"] = [job.definition.key for job in plan.jobs]
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
    definition: ArtifactDefinition,
    task: ArtifactTask,
) -> ArtifactOutput | None:
    with operation_scope(f"build:{definition.key}", task.entrypoint):
        return execute_operation(
            operation=task,
            operation_group=BUILD_OPERATIONS_EP,
            persist=lambda result: persist_artifact_output(
                result,
                artifact_key=definition.key,
                expected_relative_path=task.output,
                runtime=runtime,
                logger=logger,
            ),
            runtime=runtime,
            task_cfg=task,
        )


def _resolve_effective_settings(
    *,
    project_path: Path,
    force: bool,
    runtime_build_mode: str | None,
    cli_log_level: str | None,
    cli_visuals: str | None,
    cli_log_outputs: list[LogOutputTarget] | None,
    cli_heartbeat_interval_seconds: float | None,
    build_profile: BuildProfile | None,
    settings: BuildSettings | None,
) -> BuildSettings:
    if settings is None:
        base_level_name = str(
            logging.getLevelName(logging.getLogger().getEffectiveLevel())
        ).upper()
        try:
            settings = resolve_build_settings(
                project_path=project_path,
                cli_log_level=cli_log_level,
                cli_visuals=cli_visuals,
                cli_log_outputs=cli_log_outputs,
                cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
                force_flag=force,
                runtime_build_mode=runtime_build_mode,
                base_log_level=base_level_name,
                build_profile=build_profile,
            )
        except ValueError as exc:
            logger.error("Invalid build configuration: %s", exc)
            raise SystemExit(2) from exc
    return settings


def _plan_build(
    *,
    project_path: Path,
    settings: BuildSettings,
    build_profile: BuildProfile | None,
    required_artifacts: set[str] | None,
    artifact_task_configs: list[ArtifactTask] | None,
    artifact_graph: ArtifactGraph | None = None,
) -> BuildDecision:
    if settings.mode == "OFF" and required_artifacts is None and build_profile is None:
        return SkippedBuild(reason="mode_off", artifacts=())

    if artifact_graph is None:
        task_configs = (
            list(artifact_task_configs)
            if artifact_task_configs is not None
            else list(operation_specs(project_path)[0])
        )
        try:
            graph = build_artifact_graph(task_configs)
        except ValueError as exc:
            logger.error("%s", exc)
            raise SystemExit(2) from exc
    else:
        graph = artifact_graph

    try:
        selected_roots = graph.select_roots(
            required_artifacts=required_artifacts,
            profile_target=(
                build_profile.target if build_profile is not None else None
            ),
            profile_name=(build_profile.name if build_profile is not None else None),
        )
        selected_keys = set(graph.dependency_closure(selected_roots))
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    if not selected_keys:
        return SkippedBuild(reason="no_artifacts_selected", artifacts=())

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

    config_hash = compute_config_hash(project_path, tasks_dir(project_path))
    state_path = build_state_path(project_path)
    previous_state = load_build_state(state_path)
    freshness = graph.freshness(
        keys=selected_keys,
        state=previous_state,
        config_hash=config_hash,
        artifacts_root=artifacts_root(project_path),
        hash_meta_key=_ARTIFACT_CONFIG_HASH_META_KEY,
    )
    if settings.mode == "OFF":
        if freshness.outdated:
            artifacts = ", ".join(graph.topological_order(freshness.outdated))
            logger.error(
                "Build mode is OFF, but required artifacts are missing or stale: %s.",
                artifacts,
            )
            raise SystemExit(2)
        return SkippedBuild(
            reason="mode_off",
            artifacts=expanded_artifacts,
        )

    if not settings.force and not freshness.outdated:
        return SkippedBuild(
            reason="up_to_date",
            artifacts=expanded_artifacts,
        )

    build_keys = set(selected_keys) if settings.force else set(freshness.outdated)
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
            definition=graph.definition(key),
            task=graph.tasks_by_id[graph.definition(key).task_id],
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
            "force" if settings.force else ("missing" if freshness.missing else "stale")
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
    previous_observer = runtime.execution_observer
    install_observer = previous_observer is None
    if install_observer:
        runtime.execution_observer = make_execution_observer(
            logging.getLogger("datapipeline.dag.observer")
        )
    try:
        observer = make_operation_observer(
            logging.getLogger("datapipeline.operation.observer")
        )
        with operation_observer(observer):
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
                    definition=job.definition,
                    task=job.task,
                )
                if result is None:
                    raise RuntimeError(
                        f"Artifact task '{job.task.id}' produced no artifact."
                    )
                current_state = _merge_build_state(
                    previous_state=current_state,
                    built_artifacts={job.definition.key: result},
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
            return current_state
    finally:
        if install_observer:
            runtime.execution_observer = previous_observer


def _merge_build_state(
    previous_state: BuildState | None,
    built_artifacts: dict[str, ArtifactOutput],
    config_hash: str,
) -> BuildState:
    new_state = BuildState(config_hash=config_hash)
    if previous_state is not None:
        for key, info in previous_state.artifacts.items():
            meta = dict(info.meta or {})
            if _ARTIFACT_CONFIG_HASH_META_KEY not in meta:
                previous_hash = previous_state.config_hash
                if previous_hash:
                    meta[_ARTIFACT_CONFIG_HASH_META_KEY] = previous_hash
            new_state.artifacts[key] = ArtifactInfo(
                relative_path=info.relative_path,
                meta=meta,
            )
    for key, output in built_artifacts.items():
        meta = dict(output.meta)
        meta[_ARTIFACT_CONFIG_HASH_META_KEY] = config_hash
        new_state.register(key, output.relative_path, meta=meta)
    return new_state


def run_build_if_needed(
    project: Path | str,
    force: bool = False,
    runtime_build_mode: str | None = None,
    cli_log_level: str | None = None,
    cli_visuals: str | None = None,
    cli_log_outputs: list[LogOutputTarget] | None = None,
    cli_heartbeat_interval_seconds: float | None = None,
    required_artifacts: set[str] | None = None,
    build_profile: BuildProfile | None = None,
    artifact_task_configs: list[ArtifactTask] | None = None,
    artifact_graph: ArtifactGraph | None = None,
    settings: BuildSettings | None = None,
    skip_logging_setup: bool = False,
    runtime_override: Runtime | None = None,
) -> bool:
    """Execute artifact-producing operations when selected artifacts are missing or stale."""
    project_path = Path(project).resolve()
    settings = _resolve_effective_settings(
        project_path=project_path,
        force=force,
        runtime_build_mode=runtime_build_mode,
        cli_log_level=cli_log_level,
        cli_visuals=cli_visuals,
        cli_log_outputs=cli_log_outputs,
        cli_heartbeat_interval_seconds=cli_heartbeat_interval_seconds,
        build_profile=build_profile,
        settings=settings,
    )
    if not skip_logging_setup:
        configure_root_logging(
            level=settings.log_decision.value,
            output=settings.log_output,
        )

    plan = _plan_build(
        project_path=project_path,
        settings=settings,
        build_profile=build_profile,
        required_artifacts=required_artifacts,
        artifact_task_configs=artifact_task_configs,
        artifact_graph=artifact_graph,
    )
    _log_build_decision(
        plan,
        settings=settings,
    )
    if isinstance(plan, SkippedBuild):
        return False

    runtime = (
        runtime_override
        if runtime_override is not None
        else bootstrap_build_runtime(project_path)
    )
    if settings.heartbeat_interval_seconds is not None:
        runtime.heartbeat_interval_seconds = settings.heartbeat_interval_seconds
    _execute_build_jobs(
        runtime=runtime,
        plan=plan,
    )
    return True
