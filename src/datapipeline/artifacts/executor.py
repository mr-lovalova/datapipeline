import json
import logging
from pathlib import Path

from datapipeline.artifacts.planning import (
    build_planning_context,
    has_dataset_requirements,
    required_artifact_keys_for_dataset,
    selected_artifact_keys_for_build,
    stale_artifact_keys,
)
from datapipeline.artifacts.specs import (
    ArtifactDefinition,
    artifact_build_order,
    artifact_definition_for_key,
)
from datapipeline.build.state import (
    ArtifactInfo,
    BuildState,
    load_build_state,
    save_build_state,
)
from datapipeline.build.config_hash import compute_config_hash
from datapipeline.cli.logging_setup import configure_root_logging
from datapipeline.cli.visuals.execution import (
    emit_execution_message,
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
from datapipeline.operations.persistence import persist_artifact_output
from datapipeline.plugins import BUILD_OPERATIONS_EP
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap, build_state_path
from datapipeline.services.project_paths import tasks_dir

logger = logging.getLogger(__name__)
_ARTIFACT_CONFIG_HASH_META_KEY = "_config_hash"


def _log_build_decision(
    *,
    action: str,
    reason: str,
    settings: BuildSettings,
    selected_artifacts: int | None = None,
    expanded_artifacts: tuple[str, ...] | None = None,
    jobs: tuple[str, ...] | None = None,
    skipped_current: tuple[str, ...] | None = None,
) -> None:
    payload: dict[str, object] = {
        "action": action,
        "reason": reason,
        "mode": settings.mode,
        "profile": settings.profile_name,
    }
    if selected_artifacts is not None:
        payload["selected_artifacts"] = selected_artifacts
    if expanded_artifacts is not None:
        payload["expanded_artifacts"] = list(expanded_artifacts)
    if jobs is not None:
        payload["jobs"] = list(jobs)
    if skipped_current is not None:
        payload["skipped_current"] = list(skipped_current)
    emit_execution_message(
        f"Build decision:\n{json.dumps(payload, indent=2, default=str)}",
        level=logging.INFO,
        logger=logger,
        message_kind="build_decision",
    )


def _run_artifact_builder(
    runtime,
    definition: ArtifactDefinition,
    task: ArtifactTask,
):
    with operation_scope(f"build:{definition.key}", task.entrypoint):
        return execute_operation(
            operation=task,
            operation_group=BUILD_OPERATIONS_EP,
            persist=lambda result: persist_artifact_output(
                result,
                artifact_key=definition.key,
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
) -> dict[str, object]:
    if settings.mode == "OFF":
        return {"action": "skip", "reason": "mode_off", "selected_artifacts": None}

    task_configs = (
        list(artifact_task_configs)
        if artifact_task_configs is not None
        else list(operation_specs(project_path)[0])
    )
    context = build_planning_context(task_configs)
    definitions = context.definitions
    tasks_by_id = context.tasks_by_id

    try:
        selected_keys = selected_artifact_keys_for_build(
            context=context,
            required_artifacts=required_artifacts,
            profile_target=(build_profile.target if build_profile is not None else None),
            profile_name=(build_profile.name if build_profile is not None else None),
        )
    except ValueError as exc:
        logger.error("%s", exc)
        raise SystemExit(2) from exc

    if not selected_keys:
        return {
            "action": "skip",
            "reason": "no_artifacts_selected",
            "selected_artifacts": 0,
        }

    if has_dataset_requirements(context=context, selected_keys=selected_keys):
        dataset = load_dataset(project_path, "vectors")
        selected_keys = required_artifact_keys_for_dataset(
            context=context,
            selected_keys=selected_keys,
            dataset=dataset,
        )
    selected_count = len(selected_keys)
    expanded_artifacts = tuple(
        artifact_build_order(selected_keys, definitions=definitions)
    )
    if not selected_keys:
        return {
            "action": "skip",
            "reason": "not_required",
            "selected_artifacts": 0,
        }

    config_hash = compute_config_hash(project_path, tasks_dir(project_path))
    state_path = build_state_path(project_path)
    previous_state = load_build_state(state_path)
    stale_or_missing_required = stale_artifact_keys(
        selected_keys=selected_keys,
        state=previous_state,
        config_hash=config_hash,
        hash_meta_key=_ARTIFACT_CONFIG_HASH_META_KEY,
    )
    if not settings.force and not stale_or_missing_required:
        return {
            "action": "skip",
            "reason": "up_to_date",
            "selected_artifacts": selected_count,
            "expanded_artifacts": expanded_artifacts,
            "jobs": (),
            "skipped_current": expanded_artifacts,
        }
    missing_selected = (
        set(selected_keys)
        if previous_state is None
        else {
            key
            for key in selected_keys
            if previous_state.artifacts.get(key) is None
        }
    )

    build_keys = set(selected_keys) if settings.force else stale_or_missing_required
    job_specs: list[tuple[ArtifactDefinition, ArtifactTask]] = []
    for key in artifact_build_order(build_keys, definitions=definitions):
        definition = artifact_definition_for_key(key, definitions)
        if definition is None:
            continue
        task = tasks_by_id.get(definition.task_id)
        if task is None:
            logger.error(
                "Artifact '%s' requires task '%s', but no matching artifact task is declared.",
                definition.key,
                definition.task_id,
            )
            raise SystemExit(2)
        job_specs.append((definition, task))
    jobs = tuple(definition.key for definition, _task in job_specs)
    skipped_current = tuple(key for key in expanded_artifacts if key not in build_keys)
    return {
        "action": "run",
        "reason": "force" if settings.force else ("missing" if missing_selected else "stale"),
        "selected_artifacts": selected_count,
        "expanded_artifacts": expanded_artifacts,
        "jobs": jobs,
        "skipped_current": skipped_current,
        "config_hash": config_hash,
        "state_path": state_path,
        "previous_state": previous_state,
        "job_specs": tuple(job_specs),
    }


def _execute_build_jobs(
    *,
    runtime: Runtime,
    job_specs: tuple[tuple[ArtifactDefinition, ArtifactTask], ...],
    previous_state: BuildState | None,
    config_hash: str,
    state_path: Path,
) -> BuildState:
    previous_observer = getattr(runtime, "execution_observer", None)
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
                previous_state=previous_state,
                built_artifacts={},
                config_hash=config_hash,
            )
            for definition, task in job_specs:
                result = _run_artifact_builder(
                    runtime=runtime,
                    definition=definition,
                    task=task,
                )
                if result:
                    current_state = _merge_build_state(
                        previous_state=current_state,
                        built_artifacts={definition.key: result},
                        config_hash=config_hash,
                    )
                    save_build_state(current_state, state_path)
            return current_state
    finally:
        if install_observer:
            runtime.execution_observer = previous_observer


def _merge_build_state(
    previous_state: BuildState | None,
    built_artifacts: dict[str, dict[str, object]],
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
    for key, info in built_artifacts.items():
        relative_path = info["relative_path"]
        meta = {k: v for k, v in info.items() if k != "relative_path"}
        meta[_ARTIFACT_CONFIG_HASH_META_KEY] = config_hash
        new_state.register(key, relative_path, meta=meta)
    return new_state


def _artifact_config_hash(
    info: ArtifactInfo,
    state: BuildState,
) -> str:
    value = (info.meta or {}).get(_ARTIFACT_CONFIG_HASH_META_KEY)
    if isinstance(value, str) and value.strip():
        return value
    return state.config_hash


def _hydrate_runtime_artifacts_from_state(
    *,
    runtime: Runtime,
    state: BuildState | None,
    config_hash: str,
) -> None:
    if state is None:
        return
    artifacts = getattr(runtime, "artifacts", None)
    if artifacts is None or not hasattr(artifacts, "register"):
        return
    for key, info in state.artifacts.items():
        if _artifact_config_hash(info, state) != config_hash:
            continue
        artifacts.register(key, relative_path=info.relative_path, meta=info.meta)


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
    )
    selected_artifacts = plan.get("selected_artifacts")
    action = str(plan["action"])
    reason = str(plan["reason"])
    _log_build_decision(
        action=action,
        reason=reason,
        settings=settings,
        selected_artifacts=(
            selected_artifacts if isinstance(selected_artifacts, int) else None
        ),
        expanded_artifacts=(
            plan["expanded_artifacts"]
            if isinstance(plan.get("expanded_artifacts"), tuple)
            else None
        ),
        jobs=plan["jobs"] if isinstance(plan.get("jobs"), tuple) else None,
        skipped_current=(
            plan["skipped_current"]
            if isinstance(plan.get("skipped_current"), tuple)
            else None
        ),
    )
    if action != "run":
        return False

    job_specs = plan.get("job_specs", ())
    if not isinstance(job_specs, tuple):
        job_specs = ()
    config_hash = plan.get("config_hash")
    state_path = plan.get("state_path")
    previous_state = plan.get("previous_state")
    if not isinstance(config_hash, str) or not isinstance(state_path, Path):
        raise RuntimeError("Missing build plan state; cannot persist build results.")

    previous_state_obj = previous_state if isinstance(previous_state, BuildState) else None
    runtime = runtime_override if runtime_override is not None else bootstrap(project_path)
    if settings.heartbeat_interval_seconds is not None:
        runtime.heartbeat_interval_seconds = settings.heartbeat_interval_seconds
    _hydrate_runtime_artifacts_from_state(
        runtime=runtime,
        state=previous_state_obj,
        config_hash=config_hash,
    )
    _execute_build_jobs(
        runtime=runtime,
        job_specs=job_specs,
        previous_state=previous_state_obj,
        config_hash=config_hash,
        state_path=state_path,
    )
    return True
