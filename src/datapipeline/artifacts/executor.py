import json
import logging
from dataclasses import replace
from pathlib import Path

from datapipeline.artifacts.planning import (
    build_planning_context,
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
from datapipeline.cli.visuals.execution import make_execution_observer
from datapipeline.cli.visuals.execution import emit_execution_message
from datapipeline.cli.visuals.execution import execution_scope
from datapipeline.config.build_resolution import BuildSettings, resolve_build_settings
from datapipeline.config.profiles import BuildProfile
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.loaders.operations import operation_specs
from datapipeline.config.tasks import ArtifactTask
from datapipeline.io.output import materialized_output_message
from datapipeline.operations.dispatch import dispatch_operation
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
) -> None:
    payload: dict[str, object] = {
        "action": action,
        "reason": reason,
        "mode": settings.mode,
        "profile": settings.profile_name,
    }
    if selected_artifacts is not None:
        payload["selected_artifacts"] = selected_artifacts
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
    res = dispatch_operation(
        operation=task,
        operation_group=BUILD_OPERATIONS_EP,
        operation_type="build operation",
        runtime=runtime,
        task_cfg=task,
    )
    if not res:
        return None
    rel_path, meta = res
    full_path = (runtime.artifacts_root / rel_path).resolve()
    emit_execution_message(
        materialized_output_message(definition.key, full_path, meta=meta),
        level=logging.INFO,
        logger=logger,
        message_kind="materialized",
    )
    meta_out = {"relative_path": rel_path}
    meta_out.update(meta)
    artifacts = getattr(runtime, "artifacts", None)
    if artifacts is not None and hasattr(artifacts, "register"):
        artifacts.register(definition.key, relative_path=rel_path, meta=meta)
    return meta_out


def _resolve_effective_settings(
    *,
    project_path: Path,
    force: bool,
    runtime_build_mode: str | None,
    cli_log_level: str | None,
    cli_visuals: str | None,
    cli_log_outputs: list[LogOutputTarget] | None,
    workspace,
    build_profile: BuildProfile | None,
    settings: BuildSettings | None,
    profile_name_override: str | None,
) -> BuildSettings:
    if settings is None:
        base_level_name = str(
            logging.getLevelName(logging.getLogger().getEffectiveLevel())
        ).upper()
        try:
            settings = resolve_build_settings(
                project_path=project_path,
                workspace=workspace,
                cli_log_level=cli_log_level,
                cli_visuals=cli_visuals,
                cli_log_outputs=cli_log_outputs,
                force_flag=force,
                runtime_build_mode=runtime_build_mode,
                base_log_level=base_level_name,
                build_profile=build_profile,
            )
        except ValueError as exc:
            logger.error("Invalid log output configuration: %s", exc)
            raise SystemExit(2) from exc
    if profile_name_override is not None:
        settings = replace(settings, profile_name=profile_name_override)
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

    selected_count = len(selected_keys)
    if not selected_keys:
        return {
            "action": "skip",
            "reason": "no_artifacts_selected",
            "selected_artifacts": 0,
        }

    config_hash = compute_config_hash(project_path, tasks_dir(project_path))
    state_path = build_state_path(project_path)
    previous_state = load_build_state(state_path)
    missing_required = stale_artifact_keys(
        selected_keys=selected_keys,
        state=previous_state,
        config_hash=config_hash,
        hash_meta_key=_ARTIFACT_CONFIG_HASH_META_KEY,
    )
    if not settings.force and not missing_required:
        return {
            "action": "skip",
            "reason": "up_to_date",
            "selected_artifacts": selected_count,
        }

    job_specs: list[tuple[ArtifactDefinition, ArtifactTask]] = []
    for key in artifact_build_order(selected_keys, definitions=definitions):
        definition = artifact_definition_for_key(key, definitions)
        if definition is None:
            continue
        task = tasks_by_id.get(definition.task_id)
        if task is None:
            continue
        job_specs.append((definition, task))
    return {
        "action": "run",
        "reason": "force" if settings.force else "stale",
        "selected_artifacts": selected_count,
        "config_hash": config_hash,
        "state_path": state_path,
        "previous_state": previous_state,
        "job_specs": tuple(job_specs),
    }


def _execute_build_jobs(
    *,
    runtime: Runtime,
    job_specs: tuple[tuple[ArtifactDefinition, ArtifactTask], ...],
) -> dict[str, dict[str, object]]:
    previous_observer = getattr(runtime, "execution_observer", None)
    install_observer = previous_observer is None
    if install_observer:
        runtime.execution_observer = make_execution_observer(
            logging.getLogger("datapipeline.dag.observer")
        )
    try:
        artifacts: dict[str, dict[str, object]] = {}
        total = len(job_specs)
        for idx, (definition, task) in enumerate(job_specs, start=1):
            with execution_scope(
                phase="build",
                task_id=task.id,
                item_index=idx,
                item_total=total,
                announce=True,
            ):
                result = _run_artifact_builder(
                    runtime=runtime,
                    definition=definition,
                    task=task,
                )
            if result:
                artifacts[definition.key] = result
        return artifacts
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


def run_build_if_needed(
    project: Path | str,
    force: bool = False,
    runtime_build_mode: str | None = None,
    cli_log_level: str | None = None,
    cli_visuals: str | None = None,
    cli_log_outputs: list[LogOutputTarget] | None = None,
    workspace=None,
    required_artifacts: set[str] | None = None,
    build_profile: BuildProfile | None = None,
    artifact_task_configs: list[ArtifactTask] | None = None,
    settings: BuildSettings | None = None,
    profile_name_override: str | None = None,
    skip_logging_setup: bool = False,
    runtime_override: Runtime | None = None,
) -> bool:
    """Execute artifact-producing operations when selected artifacts are stale."""
    project_path = Path(project).resolve()
    settings = _resolve_effective_settings(
        project_path=project_path,
        force=force,
        runtime_build_mode=runtime_build_mode,
        cli_log_level=cli_log_level,
        cli_visuals=cli_visuals,
        cli_log_outputs=cli_log_outputs,
        workspace=workspace,
        build_profile=build_profile,
        settings=settings,
        profile_name_override=profile_name_override,
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

    runtime = runtime_override if runtime_override is not None else bootstrap(project_path)
    artifacts = _execute_build_jobs(
        runtime=runtime,
        job_specs=job_specs,
    )

    new_state = _merge_build_state(
        previous_state=previous_state if isinstance(previous_state, BuildState) else None,
        built_artifacts=artifacts,
        config_hash=config_hash,
    )
    save_build_state(new_state, state_path)
    return True
