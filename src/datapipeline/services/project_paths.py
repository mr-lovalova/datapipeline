from __future__ import annotations

from pathlib import Path

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
