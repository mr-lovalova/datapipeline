import json
import logging
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
from datapipeline.build.tasks import compute_config_hash
from datapipeline.cli.logging_setup import configure_root_logging
from datapipeline.cli.visuals.execution import make_execution_observer
from datapipeline.cli.visuals.execution import emit_execution_message
from datapipeline.config.build_resolution import BuildSettings, resolve_build_settings
from datapipeline.config.profiles import BuildProfile
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.loaders.operations import operation_specs
from datapipeline.config.tasks import ArtifactTask
from datapipeline.operations.dispatch import dispatch_operation
from datapipeline.plugins import BUILD_OPERATIONS_EP
from datapipeline.runtime import Runtime
from datapipeline.services.bootstrap import bootstrap, build_state_path
from datapipeline.services.project_paths import tasks_dir

logger = logging.getLogger(__name__)
_ARTIFACT_CONFIG_HASH_META_KEY = "_config_hash"


def _log_build_settings_debug(project_path: Path, settings) -> None:
    payload = {
        "project": str(project_path),
        "mode": settings.mode,
        "visuals": settings.visuals,
        "profile": settings.profile_name,
    }
    emit_execution_message(
        f"Build settings:\n{json.dumps(payload, indent=2, default=str)}",
        level=logging.DEBUG,
        logger=logger,
        message_kind="build_settings",
    )


def _log_build_status(
    *,
    status: str,
    reason: str,
    settings: BuildSettings,
    selected_artifacts: int | None = None,
) -> None:
    payload: dict[str, object] = {
        "status": status,
        "reason": reason,
        "build_profile": settings.profile_name,
        "mode": settings.mode,
    }
    if selected_artifacts is not None:
        payload["selected_artifacts"] = selected_artifacts
    emit_execution_message(
        f"Build status:\n{json.dumps(payload, indent=2, default=str)}",
        level=logging.INFO,
        logger=logger,
        message_kind="build_status",
    )


def _log_task_config_debug(
    definition: ArtifactDefinition,
    task: ArtifactTask,
    idx: int,
    total: int,
) -> None:
    payload = {
        "idx": idx,
        "total": total,
        "id": task.id,
        "artifact": definition.key,
        "output": getattr(task, "output", None),
    }
    emit_execution_message(
        f"Build task config:\n{json.dumps(payload, indent=2, default=str)}",
        level=logging.DEBUG,
        logger=logger,
        message_kind="task_config",
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
    details = ", ".join(f"{k}={v}" for k, v in meta.items())
    suffix = f" ({details})" if details else ""
    emit_execution_message(
        f"Materialized {definition.key}: {full_path}{suffix}",
        level=logging.INFO,
        logger=logger,
        message_kind="materialized",
    )
    meta_out = {"relative_path": rel_path}
    meta_out.update(meta)
    return meta_out


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
    cli_log_level: str | None = None,
    cli_visuals: str | None = None,
    cli_log_outputs: list[LogOutputTarget] | None = None,
    workspace=None,
    required_artifacts: set[str] | None = None,
    build_profile: BuildProfile | None = None,
    artifact_task_configs: list[ArtifactTask] | None = None,
    settings: BuildSettings | None = None,
    skip_logging_setup: bool = False,
    runtime_override: Runtime | None = None,
) -> bool:
    """Execute artifact-producing operations when selected artifacts are stale."""
    project_path = Path(project).resolve()
    base_level_name = str(
        logging.getLevelName(logging.getLogger().getEffectiveLevel())
    ).upper()
    if settings is None:
        try:
            settings = resolve_build_settings(
                project_path=project_path,
                workspace=workspace,
                cli_log_level=cli_log_level,
                cli_visuals=cli_visuals,
                cli_log_outputs=cli_log_outputs,
                force_flag=force,
                base_log_level=base_level_name,
                build_profile=build_profile,
            )
        except ValueError as exc:
            logger.error("Invalid log output configuration: %s", exc)
            raise SystemExit(2) from exc
    effective_level = settings.log_decision.value
    if not skip_logging_setup:
        configure_root_logging(level=effective_level, output=settings.log_output)

    if settings.mode == "OFF":
        _log_build_status(
            status="skipped",
            reason="mode_off",
            settings=settings,
        )
        return False
    force = settings.force
    tasks_root = tasks_dir(project_path)
    config_hash = compute_config_hash(project_path, tasks_root)

    state_path = build_state_path(project_path)
    state = load_build_state(state_path)

    _log_build_settings_debug(project_path, settings)

    if artifact_task_configs is not None:
        task_configs = list(artifact_task_configs)
    else:
        task_configs, _ = operation_specs(project_path)
        task_configs = list(task_configs)
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
        _log_build_status(
            status="skipped",
            reason="no_artifacts_selected",
            settings=settings,
            selected_artifacts=0,
        )
        return False

    missing_required = stale_artifact_keys(
        selected_keys=selected_keys,
        state=state,
        config_hash=config_hash,
        hash_meta_key=_ARTIFACT_CONFIG_HASH_META_KEY,
    )
    if not force and not missing_required:
        _log_build_status(
            status="skipped",
            reason="up_to_date",
            settings=settings,
            selected_artifacts=len(selected_keys),
        )
        return False

    runtime = runtime_override if runtime_override is not None else bootstrap(project_path)
    job_specs: list[tuple[ArtifactDefinition, ArtifactTask]] = []
    for key in artifact_build_order(selected_keys, definitions=definitions):
        definition = artifact_definition_for_key(key, definitions)
        if definition is None:
            continue
        task = tasks_by_id.get(definition.task_id)
        if task is None:
            continue
        job_specs.append((definition, task))

    previous_observer = getattr(runtime, "execution_observer", None)
    install_observer = previous_observer is None
    if install_observer:
        runtime.execution_observer = make_execution_observer(
            logging.getLogger("datapipeline.dag.observer")
        )
    try:
        artifacts: dict[str, dict[str, object]] = {}
        total_jobs = len(job_specs)
        for idx, (definition, task) in enumerate(job_specs, start=1):
            _log_task_config_debug(
                definition=definition,
                task=task,
                idx=idx,
                total=total_jobs,
            )
            result = _run_artifact_builder(
                runtime=runtime,
                definition=definition,
                task=task,
            )
            if result:
                artifacts[definition.key] = result
    finally:
        if install_observer:
            runtime.execution_observer = previous_observer

    new_state = _merge_build_state(
        previous_state=state,
        built_artifacts=artifacts,
        config_hash=config_hash,
    )
    save_build_state(new_state, state_path)
    return True
