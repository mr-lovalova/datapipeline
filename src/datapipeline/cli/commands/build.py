import json
import logging
from pathlib import Path
from typing import Callable, Optional

from datapipeline.build.state import BuildState, load_build_state, save_build_state
from datapipeline.build.tasks import (
    compute_config_hash,
    materialize_partitioned_ids,
    materialize_scaler_statistics,
)
from datapipeline.cli.visuals import get_visuals_backend
from datapipeline.cli.visuals.runner import run_job
from datapipeline.cli.visuals.sections import sections_from_path
from datapipeline.config.build import load_build_config
from datapipeline.config.context import resolve_build_settings
from datapipeline.services.bootstrap import artifacts_root, bootstrap
from datapipeline.services.constants import (
    PARTIONED_IDS,
    PARTITIONED_TARGET_IDS,
    SCALER_STATISTICS,
)
from datapipeline.services.project_paths import build_config_path


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
    logger.debug("Build settings:\n%s", json.dumps(payload, indent=2, default=str))


def _log_build_config_debug(build_config) -> None:
    if not logger.isEnabledFor(logging.DEBUG):
        return
    logger.debug(
        "Build config:\n%s",
        json.dumps(
            build_config.model_dump(exclude_none=True),
            indent=2,
            default=str,
        ),
    )


def run_build_if_needed(
    project: Path | str,
    *,
    force: bool = False,
    cli_visuals: str | None = None,
    cli_progress: str | None = None,
    workspace=None,
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
    cfg_path = build_config_path(project_path)
    config_hash = compute_config_hash(project_path, cfg_path)

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

    if state and (state.config_hash == config_hash) and not force:
        logger.info(
            "Build is up-to-date (config hash matches); skipping rebuild.")
        return False
    build_root = build_config_path(project_path)
    build_config = load_build_config(project_path)
    _log_build_config_debug(build_config)
    runtime = bootstrap(project_path)

    artifacts = {}

    def _work_scaler():
        try:
            logger.info(
                "Building artifact: scaler -> %s", build_config.scaler.output
            )
        except Exception:
            pass
        res = materialize_scaler_statistics(runtime, build_config)
        if not res:
            return None
        rel_path, meta = res
        meta_out = {"relative_path": rel_path}
        meta_out.update(meta)
        return meta_out

    job_specs: list[tuple[str, str, Callable[[], object], Optional[Path]]] = []
    seen_targets: set[str] = set()

    def _make_partition_job(task_cfg):
        def _work():
            try:
                logger.info(
                    "Building artifact: partitioned_ids (%s) -> %s",
                    task_cfg.target,
                    task_cfg.output,
                )
            except Exception:
                pass
            rel_path, count = materialize_partitioned_ids(runtime, task_cfg)
            return {"relative_path": rel_path, "count": count}

        return _work

    for idx, task_cfg in enumerate(build_config.partitioned_ids, start=1):
        label = f"partitioned_ids[{task_cfg.target}:{idx}]"
        config_path = task_cfg.source_path or (build_root / "partitioned_ids.yaml")
        artifact_key = PARTIONED_IDS if task_cfg.target == "features" else PARTITIONED_TARGET_IDS
        if artifact_key in seen_targets:
            logger.error(
                "Multiple partitioned_ids artifacts target '%s'; only one per target is supported.",
                task_cfg.target,
            )
            raise SystemExit(2)
        seen_targets.add(artifact_key)
        job_specs.append((label, artifact_key, _make_partition_job(task_cfg), config_path))

    if getattr(build_config.scaler, "enabled", True):
        job_specs.append(("scaler", SCALER_STATISTICS, _work_scaler, build_root / "scaler.yaml"))

    total_jobs = len(job_specs)
    for idx, (job_label, artifact_key, job_work, config_path) in enumerate(job_specs, start=1):
        sections = sections_from_path(build_root, config_path)
        result = run_job(
            sections=sections,
            label=job_label,
            visuals=effective_provider,
            progress_style=effective_style,
            level=effective_level,
            runtime=runtime,
            work=job_work,
            idx=idx,
            total=total_jobs,
        )
        if result:
            artifacts[artifact_key] = result

    new_state = BuildState(config_hash=config_hash)
    for key, info in artifacts.items():
        relative_path = info["relative_path"]
        meta = {k: v for k, v in info.items() if k != "relative_path"}
        new_state.register(key, relative_path, meta=meta)
        details = ", ".join(f"{k}={v}" for k, v in meta.items())
        suffix = f" ({details})" if details else ""
        logger.info("Materialized %s -> %s%s", key, relative_path, suffix)

    save_build_state(new_state, state_path)
    logger.info("Build completed.")
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
