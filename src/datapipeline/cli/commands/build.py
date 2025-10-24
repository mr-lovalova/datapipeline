from pathlib import Path

from datapipeline.build.state import BuildState, load_build_state, save_build_state
from datapipeline.build.tasks import compute_config_hash, execute_build
from datapipeline.config.build import load_build_config
from datapipeline.services.bootstrap import artifacts_root, bootstrap
from datapipeline.services.project_paths import build_config_path


def handle(project: str, *, force: bool = False) -> None:
    """Materialize build artifacts for the configured project."""

    project_path = Path(project).resolve()
    cfg_path = build_config_path(project_path)
    build_config = load_build_config(project_path)
    config_hash = compute_config_hash(project_path, cfg_path)

    art_root = artifacts_root(project_path)
    state_path = (art_root / "build" / "state.json").resolve()
    state = load_build_state(state_path)

    if state and (state.config_hash == config_hash) and not force:
        print("✅ Build is up-to-date (config hash matches). Use --force to rebuild.")
        return

    runtime = bootstrap(project_path)
    artifacts = execute_build(runtime, build_config)

    new_state = BuildState(config_hash=config_hash)
    for key, info in artifacts.items():
        relative_path = info["relative_path"]
        meta = {k: v for k, v in info.items() if k != "relative_path"}
        new_state.register(key, relative_path, meta=meta)
        details = ", ".join(f"{k}={v}" for k, v in meta.items())
        suffix = f" ({details})" if details else ""
        print(f"🛠️  {key} → {relative_path}{suffix}")

    save_build_state(new_state, state_path)
    print("✅ Build completed.")
