import json
import logging
from pathlib import Path

from datapipeline.build.state import BuildState, load_build_state, save_build_state
from datapipeline.build.tasks import compute_config_hash
from datapipeline.cli.visuals import get_visuals_backend
from datapipeline.cli.visuals.runner import run_job
from datapipeline.cli.visuals.sections import sections_from_path
from datapipeline.config.tasks import ArtifactTask, artifact_tasks
from datapipeline.config.context import resolve_build_settings
from datapipeline.artifacts.specs import (
    ArtifactDefinition,
    artifact_build_order,
    artifact_definition_for_key,
    artifact_keys_for_task_kinds,
)
from datapipeline.services.bootstrap import artifacts_root, bootstrap
from datapipeline.services.project_paths import tasks_dir


logger = logging.getLogger(__name__)


def _log_build_settings_debug(project_path: Path, settings) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return
    payload = {
        "project": str(project_path),
        "mode": settings.mode,
        "force": settings.force,
        "visuals": settings.visuals,
        "progress": settings.progress,
    }
    logger.debug("Build settings:\n%s", json.dumps(
        payload, indent=2, default=str))


def _log_task_overview(tasks: list[ArtifactTask]) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return
    payload = [
        {
            "name": task.effective_name(),
            "kind": task.kind,
            "enabled": task.enabled,
            "output": getattr(task, "output", None),
        }
        for task in tasks
    ]
    logger.debug("Artifact tasks:\n%s", json.dumps(payload, indent=2, default=str))


def _run_artifact_builder(
    *,
    runtime,
    definition: ArtifactDefinition,
    task: ArtifactTask,
):
    res = definition.run(runtime, task)
    if not res:
        return None
    rel_path, meta = res
    full_path = (runtime.artifacts_root / rel_path).resolve()
    details = ", ".join(f"{k}={v}" for k, v in meta.items())
    suffix = f" ({details})" if details else ""
    logger.info("Materialized %s -> %s%s", definition.key, full_path, suffix)
    meta_out = {"relative_path": rel_path}
    meta_out.update(meta)
    return meta_out


def run_build_if_needed(
    project: Path | str,
    *,
    force: bool = False,
    cli_visuals: str | None = None,
    cli_progress: str | None = None,
    workspace=None,
    required_artifacts: set[str] | None = None,
) -> bool:
    """Execute the build workflow when the cached config hash has changed.

    Returns True when a build was performed, False if skipped.
    """
    project_path = Path(project).resolve()
    settings = resolve_build_settings(
        workspace=workspace,
        cli_visuals=cli_visuals,
        cli_progress=cli_progress,
        force_flag=force,
    )
    effective_provider = settings.visuals
    effective_style = settings.progress

    if settings.mode == "OFF":
        logger.info("Build skipped (jerry.yaml build.mode=OFF).")
        return False
    force = settings.force
    tasks_root = tasks_dir(project_path)
    config_hash = compute_config_hash(project_path, tasks_root)

    art_root = artifacts_root(project_path)
    state_path = (art_root / "build" / "state.json").resolve()
    state = load_build_state(state_path)

    effective_level = logging.getLogger().getEffectiveLevel()
    backend = get_visuals_backend(effective_provider)
    # Present headline before deciding to skip or run
    try:
        handled = backend.on_build_start(project_path)
    except Exception:
        handled = False
    if not handled:
        from os import getcwd as _getcwd
        try:
            cwd = Path(_getcwd())
            rel = project_path.relative_to(cwd)
            parts = [part for part in rel.as_posix().split("/") if part]
        except Exception:
            parts = [part for part in project_path.as_posix().split("/")
                     if part]
        if len(parts) > 3:
            parts = ["..."] + parts[-3:]
        compact = "/".join(parts) if parts else project_path.name
        logger.info("project: %s", compact)

    _log_build_settings_debug(project_path, settings)

    missing_required = set(required_artifacts or [])
    if missing_required:
        existing = state.artifacts.keys() if state else set()
        missing_required = {art for art in missing_required if art not in existing}
    if state and (state.config_hash == config_hash) and not force and not missing_required:
        logger.info(
            "Build is up-to-date (config hash matches); skipping rebuild.")
        return False
    if required_artifacts is not None and not required_artifacts:
        logger.info("Build skipped (no artifacts required for this run).")
        return False

    task_configs = artifact_tasks(project_path)
    _log_task_overview(task_configs)
    runtime = bootstrap(project_path)

    tasks_by_kind = {
        task.kind: task
        for task in task_configs
        if task.enabled
    }

    artifacts = {}

    if required_artifacts is None:
        selected_keys = artifact_keys_for_task_kinds(tasks_by_kind.keys())
    else:
        selected_keys = set(required_artifacts)

    ordered_keys = artifact_build_order(selected_keys)
    job_specs: list[tuple[ArtifactDefinition, ArtifactTask]] = []
    for key in ordered_keys:
        definition = artifact_definition_for_key(key)
        if definition is None:
            continue
        task = tasks_by_kind.get(definition.task_kind)
        if task is None:
            continue
        if not definition.supports(task):
            raise TypeError(
                f"Artifact '{definition.key}' does not support task type "
                f"{type(task).__name__}"
            )
        job_specs.append((definition, task))

    total_jobs = len(job_specs)
    for idx, (definition, task) in enumerate(job_specs, start=1):
        # Prefix sections with a phase label for visuals; keep path-based detail.
        path_sections = sections_from_path(tasks_root, task.source_path or tasks_root)
        sections = ("Build Tasks",) + tuple(path_sections[1:])
        job_label = definition.task_kind
        result = run_job(
            sections=sections,
            label=job_label,
            visuals=effective_provider,
            progress_style=effective_style,
            level=effective_level,
            runtime=runtime,
            work=lambda definition=definition, task=task: _run_artifact_builder(
                runtime=runtime,
                definition=definition,
                task=task,
            ),
            idx=idx,
            total=total_jobs,
        )
        if result:
            artifacts[definition.key] = result

    new_state = BuildState(config_hash=config_hash)
    for key, info in artifacts.items():
        relative_path = info["relative_path"]
        meta = {k: v for k, v in info.items() if k != "relative_path"}
        new_state.register(key, relative_path, meta=meta)

    save_build_state(new_state, state_path)
    return True


def handle(
    project: str,
    *,
    force: bool = False,
    cli_visuals: str | None = None,
    cli_progress: str | None = None,
    workspace=None,
) -> None:
    """Materialize build artifacts for the configured project."""
    run_build_if_needed(
        project,
        force=force,
        cli_visuals=cli_visuals,
        cli_progress=cli_progress,
        workspace=workspace,
    )
