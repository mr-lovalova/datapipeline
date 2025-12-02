import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from datapipeline.config.project import ProjectConfig
from datapipeline.utils.load import load_yaml
from datapipeline.utils.placeholders import MissingInterpolation, is_missing


def _serialize_global_value(value: Any) -> Any:
    """Normalize project global values for interpolation."""
    if isinstance(value, datetime):
        try:
            return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return value.isoformat()
    if value is None:
        return None
    return str(value)


def _project(project_yaml: Path) -> ProjectConfig:
    """Load and validate project.yaml."""
    data = load_yaml(project_yaml)
    vars_ = _project_vars(data)
    paths = data.get("paths")
    if isinstance(paths, dict) and vars_:
        data["paths"] = _interpolate(paths, vars_)
    return ProjectConfig.model_validate(data)


def _paths(project_yaml: Path) -> Mapping[str, str]:
    proj = _project(project_yaml)
    return proj.paths.model_dump()


def _project_vars(data: dict) -> dict[str, Any]:
    vars_: dict[str, Any] = {}
    name = data.get("name")
    if name:
        vars_["project"] = str(name)
        vars_["project_name"] = str(name)

    version = data.get("version")
    if version is not None:
        vars_["version"] = str(version)
        vars_["project_version"] = str(version)

    globals_ = data.get("globals") or {}
    for k, v in globals_.items():
        vars_[str(k)] = _serialize_global_value(v)
    return vars_


def artifacts_root(project_yaml: Path) -> Path:
    """Return the artifacts directory for a given project.yaml.

    Single source of truth: project.paths.artifacts must be provided.
    If relative, it is resolved against the folder containing project.yaml.
    """
    pj = project_yaml.resolve()
    paths = _paths(project_yaml)
    a = paths.get("artifacts")
    if not a:
        raise ValueError(
            "project.paths.artifacts must be set (absolute or relative to project.yaml)"
        )
    ap = Path(a)
    return (pj.parent / ap).resolve() if not ap.is_absolute() else ap


def run_root(project_yaml: Path, run_id: str | None = None) -> Path:
    """Return a per-run artifacts directory under the project artifacts root.

    Example:
      artifacts_root: /.../artifacts/my_dataset/v3
      run_root:       /.../artifacts/my_dataset/v3/runs/2025-11-29T14-15-23Z
    """
    base = artifacts_root(project_yaml)

    if run_id is None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        run_id = ts

    root = (base / "runs" / run_id).resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


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
    path = Path(p)
    if not path.is_absolute():
        path = project_yaml.parent / path
    return load_yaml(path, require_mapping=require_mapping)


def _globals(project_yaml: Path) -> dict[str, Any]:
    """Return project-level globals for interpolation.

    If a value is a datetime, normalize to strict UTC Z-format string so
    downstream components expecting ISO Z will work predictably.
    Preserve explicit nulls; otherwise coerce to string.
    """
    proj = _project(project_yaml)
    g = proj.globals.model_dump()
    out: dict[str, Any] = {}
    for k, v in g.items():
        out[str(k)] = _serialize_global_value(v)
    return out


_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _interpolate(obj, vars_: dict[str, Any]):
    """Recursively substitute ${var} in strings using vars_ map.

    Minimal behavior: if a key is missing, leave placeholder as-is.
    """
    if isinstance(obj, dict):
        return {k: _interpolate(v, vars_) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate(v, vars_) for v in obj]
    if isinstance(obj, str):
        match = _VAR_RE.fullmatch(obj)
        if match:
            key = match.group(1)
            if key in vars_:
                value = vars_[key]
                if value is None or is_missing(value):
                    return MissingInterpolation(key)
                return str(value)
            return obj

        def repl(m):
            key = m.group(1)
            value = vars_.get(key, m.group(0))
            if value is None or is_missing(value):
                return m.group(0)
            return str(value)

        return _VAR_RE.sub(repl, obj)
    return obj


__all__ = [
    "artifacts_root",
    "run_root",
    "_globals",
    "_interpolate",
    "_load_by_key",
    "_paths",
    "_project",
    "_project_vars",
    "_serialize_global_value",
]
