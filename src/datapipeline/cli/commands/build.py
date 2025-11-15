import logging
from pathlib import Path
from typing import Callable

from datapipeline.build.state import BuildState, load_build_state, save_build_state
from datapipeline.build.tasks import (
    compute_config_hash,
    materialize_partitioned_ids,
    materialize_scaler_statistics,
)
from datapipeline.cli.visuals import get_visuals_backend
from datapipeline.config.build import load_build_config
from datapipeline.config.run import load_build_runtime_config
from datapipeline.services.bootstrap import artifacts_root, bootstrap
from datapipeline.services.project_paths import build_config_path
from datapipeline.cli.visuals.runner import run_job


logger = logging.getLogger(__name__)


def run_build_if_needed(
    project: Path | str,
    *,
    force: bool = False,
    ensure_level: int | None = None,
    cli_visual_provider: str | None = None,
    cli_progress_style: str | None = None,
) -> bool:
    """Execute the build workflow when the cached config hash has changed.

    Returns True when a build was performed, False if skipped.
    """
    project_path = Path(project).resolve()
    runtime_overrides = load_build_runtime_config(project_path)
    effective_provider = cli_visual_provider
    if (
        effective_provider is None
        and runtime_overrides
        and runtime_overrides.visual_provider
    ):
        effective_provider = runtime_overrides.visual_provider.lower()
    effective_provider = effective_provider or "auto"

    effective_style = cli_progress_style
    if effective_style is None and runtime_overrides and runtime_overrides.progress_style:
        effective_style = runtime_overrides.progress_style.lower()
    effective_style = effective_style or "auto"

    effective_mode = runtime_overrides.mode if runtime_overrides else "AUTO"
    if force:
        effective_mode = "FORCE"
    if effective_mode == "OFF":
        logger.info("Build skipped (runtime.build.yaml mode=OFF).")
        return False
    force = force or effective_mode == "FORCE"
    cfg_path = build_config_path(project_path)
    config_hash = compute_config_hash(project_path, cfg_path)

    art_root = artifacts_root(project_path)
    state_path = (art_root / "build" / "state.json").resolve()
    state = load_build_state(state_path)

    root_logger = logging.getLogger()
    original_level = root_logger.level
    level_changed = False

    effective_ensure = ensure_level
    if runtime_overrides and runtime_overrides.log_level:
        cfg_level = logging._nameToLevel.get(
            runtime_overrides.log_level.upper())
        if cfg_level is not None:
            if effective_ensure is None or cfg_level < effective_ensure:
                effective_ensure = cfg_level

    if effective_ensure is not None:
        effective_level = root_logger.getEffectiveLevel()
        if effective_level > effective_ensure:
            root_logger.setLevel(effective_ensure)
            level_changed = True

    try:
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
            logger.info("Build: %s", compact)

        if state and (state.config_hash == config_hash) and not force:
            logger.info(
                "Build is up-to-date (config hash matches); skipping rebuild.")
            return False
        build_config = load_build_config(project_path)
        runtime = bootstrap(project_path)
        effective_level = logging.getLogger().getEffectiveLevel()

        artifacts = {}

        def _work_ids():
            try:
                logger.info(
                    "Building artifact: partitioned_ids -> %s",
                    build_config.partitioned_ids.output,
                )
            except Exception:
                pass
            rel_path, count = materialize_partitioned_ids(
                runtime, build_config)
            return {"relative_path": rel_path, "count": count}

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

        job_specs: list[tuple[str, Callable[[], object]]] = [
            ("partitioned_ids", _work_ids)]
        if getattr(build_config.scaler, "enabled", True):
            job_specs.append(("scaler", _work_scaler))

        total_jobs = len(job_specs)
        for idx, (job_label, job_work) in enumerate(job_specs, start=1):
            result = run_job(
                kind="artifact",
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
                artifacts[job_label] = result

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
    finally:
        if level_changed:
            root_logger.setLevel(original_level)


def handle(
    project: str,
    *,
    force: bool = False,
    cli_visual_provider: str | None = None,
    cli_progress_style: str | None = None,
) -> None:
    """Materialize build artifacts for the configured project."""
    run_build_if_needed(
        project,
        force=force,
        cli_visual_provider=cli_visual_provider,
        cli_progress_style=cli_progress_style,
    )
