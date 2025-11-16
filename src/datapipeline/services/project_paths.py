from __future__ import annotations

from pathlib import Path
from typing import Optional

from datapipeline.utils.load import load_yaml
from datapipeline.config.project import ProjectConfig


def read_project(project_yaml: Path) -> ProjectConfig:
    data = load_yaml(project_yaml)
    return ProjectConfig.model_validate(data)


def _project_root(project_yaml: Path) -> Path:
    return project_yaml.parent


def streams_dir(project_yaml: Path) -> Path:
    cfg = read_project(project_yaml)
    p = Path(cfg.paths.streams)
    if not p.is_absolute():
        p = _project_root(project_yaml) / p
    if not p.exists() or not p.is_dir():
        raise FileNotFoundError(f"streams dir not found: {p}")
    return p


def sources_dir(project_yaml: Path) -> Path:
    cfg = read_project(project_yaml)
    p = Path(cfg.paths.sources)
    if not p.is_absolute():
        p = _project_root(project_yaml) / p
    if not p.exists() or not p.is_dir():
        raise FileNotFoundError(f"sources dir not found: {p}")
    return p


def build_config_path(project_yaml: Path) -> Path:
    """Return the resolved path to the build artifacts directory (project.paths.build)."""

    cfg = read_project(project_yaml)
    build_path = getattr(cfg.paths, "build", None)
    if not build_path:
        raise FileNotFoundError(
            "project.paths.build must point to a build artifacts directory."
        )
    p = Path(build_path)
    if not p.is_absolute():
        p = _project_root(project_yaml) / p
    if not p.exists() or not p.is_dir():
        raise FileNotFoundError(f"build config not found: {p}")
    return p.resolve()


def ensure_project_scaffold(project_yaml: Path) -> None:
    """Ensure a minimal project scaffold exists.

    - Creates parent directories and a default project.yaml if missing.
    - Ensures the `paths.streams` and `paths.sources` directories exist.
    - Never overwrites existing files.
    """
    # Create default project.yaml if missing
    if not project_yaml.exists():
        project_yaml.parent.mkdir(parents=True, exist_ok=True)
        default = (
            "version: 1\n"
            "paths:\n"
            "  streams: ../../contracts\n"
            "  sources: ../../sources\n"
            "  dataset: dataset.yaml\n"
            "  postprocess: postprocess.yaml\n"
            "  artifacts: ../../build/datasets/default\n"
            "  build: build/artifacts\n"
            "  run: runs\n"
            "globals:\n"
            "  start_time: 2021-01-01T00:00:00Z\n"
            "  end_time: 2021-12-31T23:00:00Z\n"
        )
        project_yaml.write_text(default, encoding="utf-8")

    # Ensure paths exist based on the (possibly newly created) project file
    try:
        cfg = read_project(project_yaml)
        streams = Path(cfg.paths.streams)
        if not streams.is_absolute():
            streams = _project_root(project_yaml) / streams
        streams.mkdir(parents=True, exist_ok=True)

        sources = Path(cfg.paths.sources)
        if not sources.is_absolute():
            sources = _project_root(project_yaml) / sources
        sources.mkdir(parents=True, exist_ok=True)
    except Exception:
        # If the file is malformed, leave it to callers to report; this helper
        # is best-effort to create a sensible starting point.
        pass


def resolve_project_yaml_path(
    plugin_root: Path,
    config_root: Optional[Path],
) -> Path:
    """Return project.yaml path honoring optional workspace config overrides."""
    if config_root:
        target = Path(config_root)
        if target.suffix.lower() in {".yaml", ".yml"}:
            return target
        candidate = target / "project.yaml"
        if candidate.exists():
            return candidate
        fallback = target / "datasets" / "default" / "project.yaml"
        if fallback.exists():
            return fallback
        return candidate
    return plugin_root / "config" / "datasets" / "default" / "project.yaml"
