import logging
from pathlib import Path

from tqdm.contrib.logging import logging_redirect_tqdm

from datapipeline.build.state import BuildState, load_build_state, save_build_state
from datapipeline.build.tasks import compute_config_hash, execute_build
from datapipeline.cli.visuals import visual_sources
from datapipeline.config.build import load_build_config
from datapipeline.services.bootstrap import artifacts_root, bootstrap
from datapipeline.services.project_paths import build_config_path


logger = logging.getLogger(__name__)


def run_build_if_needed(
    project: Path | str,
    *,
    force: bool = False,
    ensure_level: int | None = None,
) -> bool:
    """Execute the build workflow when the cached config hash has changed.

    Returns True when a build was performed, False if skipped.
    """
    project_path = Path(project).resolve()
    cfg_path = build_config_path(project_path)
    config_hash = compute_config_hash(project_path, cfg_path)

    art_root = artifacts_root(project_path)
    state_path = (art_root / "build" / "state.json").resolve()
    state = load_build_state(state_path)

    root_logger = logging.getLogger()
    original_level = root_logger.level
    level_changed = False

    if ensure_level is not None:
        effective_level = root_logger.getEffectiveLevel()
        if effective_level > ensure_level:
            root_logger.setLevel(ensure_level)
            level_changed = True

    try:
        if state and (state.config_hash == config_hash) and not force:
            logger.info("Build is up-to-date (config hash matches); skipping rebuild.")
            return False

        build_config = load_build_config(project_path)
        logger.info("Starting build for %s", project_path)

        runtime = bootstrap(project_path)
        effective_level = logging.getLogger().getEffectiveLevel()

        with visual_sources(runtime, effective_level):
            with logging_redirect_tqdm():
                artifacts = execute_build(runtime, build_config)

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


def handle(project: str, *, force: bool = False) -> None:
    """Materialize build artifacts for the configured project."""
    run_build_if_needed(project, force=force)
