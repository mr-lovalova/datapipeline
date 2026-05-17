from pathlib import Path
from typing import Any, Mapping

from datapipeline.config.project import ProjectConfig
from datapipeline.services.config_refs import (
    interpolate_config_vars,
    project_vars_from_data,
    resolve_config_refs,
    serialize_project_value,
)
from datapipeline.services.path_policy import resolve_project_path
from datapipeline.utils.load import load_yaml


def _project(project_yaml: Path) -> ProjectConfig:
    """Load and validate project.yaml."""
    data = resolve_config_refs(load_yaml(project_yaml), project_yaml=project_yaml)
    vars_ = _project_vars(data)
    paths = data.get("paths")
    if isinstance(paths, dict) and vars_:
        data["paths"] = interpolate_config_vars(paths, vars_)
    return ProjectConfig.model_validate(data)


def _paths(project_yaml: Path) -> Mapping[str, str]:
    proj = _project(project_yaml)
    return proj.paths.model_dump()


def _project_vars(data: dict) -> dict[str, Any]:
    return project_vars_from_data(data)


def artifacts_root(project_yaml: Path) -> Path:
    """Return the artifacts directory for a given project.yaml.

    Single source of truth: project.paths.artifacts must be provided.
    If relative, it is resolved against the folder containing project.yaml.
    """
    paths = _paths(project_yaml)
    a = paths.get("artifacts")
    if not a:
        raise ValueError(
            "project.paths.artifacts must be set (absolute or relative to project.yaml)"
        )
    return resolve_project_path(project_yaml, a)


def build_state_path(project_yaml: Path) -> Path:
    """Return the internal build-state ledger path."""
    return (artifacts_root(project_yaml) / "_system" / "build" / "state.json").resolve()


def _load_by_key(
    project_yaml: Path,
    key: str,
    *,
    require_mapping: bool = True,
) -> Any:
    """Load a YAML document referenced by project.paths[key]. (Legacy)"""
    p = _paths(project_yaml).get(key)
    if not p:
        raise FileNotFoundError(f"project.paths must include '{key}'.")
    path = resolve_project_path(project_yaml, p)
    return resolve_config_refs(
        load_yaml(path, require_mapping=require_mapping),
        project_yaml=project_yaml,
    )


def _globals(project_yaml: Path) -> dict[str, Any]:
    """Return project-level globals for interpolation.

    If a value is a datetime, normalize to strict UTC Z-format string so
    downstream components expecting ISO Z will work predictably.
    Preserve explicit nulls; otherwise coerce to string.
    """
    # Validate the project first, then return globals from the same project-var
    # resolver used for paths/source interpolation. This keeps nested globals
    # consistent across all config surfaces.
    _project(project_yaml)
    data = resolve_config_refs(load_yaml(project_yaml), project_yaml=project_yaml)
    g = data.get("globals") or {}
    if not isinstance(g, Mapping):
        return {}
    vars_ = _project_vars(data)
    out: dict[str, Any] = {}
    for k in g:
        key = str(k)
        out[key] = vars_.get(key)
    return out


def _interpolate(obj, vars_: dict[str, Any]):
    return interpolate_config_vars(obj, vars_)


__all__ = [
    "artifacts_root",
    "build_state_path",
    "_globals",
    "_interpolate",
    "_load_by_key",
    "_paths",
    "_project",
    "_project_vars",
]
