from __future__ import annotations

from pathlib import Path

from datapipeline.utils.load import load_yaml
from datapipeline.config.project import ProjectConfig


def read_project(project_yaml: Path) -> ProjectConfig:
    data = load_yaml(project_yaml)
    return ProjectConfig.model_validate(data)


def _project_root(project_yaml: Path) -> Path:
    return project_yaml.parent.parent


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
