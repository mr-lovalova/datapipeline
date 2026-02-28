import importlib
import importlib.metadata as md
import tomllib
from functools import lru_cache
from pathlib import Path

import yaml

# Local fallback map so newly added entrypoints remain usable in editable installs
# before package metadata is refreshed.
_EP_OVERRIDES = {}


@lru_cache
def _local_project_entrypoints(group: str) -> dict[str, str]:
    repo_root = Path(__file__).resolve().parents[3]
    pyproject = repo_root / "pyproject.toml"
    if not pyproject.exists():
        return {}
    try:
        data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    except Exception:
        return {}
    project = data.get("project", {})
    entrypoints = project.get("entry-points", {})
    group_values = entrypoints.get(group, {})
    if not isinstance(group_values, dict):
        return {}
    result: dict[str, str] = {}
    for key, value in group_values.items():
        if isinstance(key, str) and isinstance(value, str):
            result[key] = value
    return result


@lru_cache
def load_ep(group: str, name: str):
    target = _EP_OVERRIDES.get((group, name))
    if target:
        module, attr = target.split(":")
        return getattr(importlib.import_module(module), attr)

    eps = md.entry_points().select(group=group, name=name)
    if not eps:
        local_target = _local_project_entrypoints(group).get(name)
        if local_target:
            module, attr = local_target.split(":")
            return getattr(importlib.import_module(module), attr)
        available = ", ".join(
            sorted(ep.name for ep in md.entry_points().select(group=group))
        )
        raise ValueError(
            f"No entry point '{name}' in '{group}'. Available: {available or '(none)'}")
    if len(eps) > 1:
        def describe(ep):
            value = getattr(ep, "value", None)
            if value:
                return value
            module = getattr(ep, "module", None)
            attr = getattr(ep, "attr", None)
            if module and attr:
                return f"{module}:{attr}"
            return repr(ep)
        mods = ", ".join(describe(ep) for ep in eps)
        raise ValueError(
            f"Ambiguous entry point '{name}' in '{group}': {mods}")
    # EntryPoints in newer Python versions are mapping-like; avoid integer indexing
    ep = next(iter(eps))
    return ep.load()


def load_yaml(p: Path, require_mapping: bool = True):
    try:
        with p.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"YAML file not found: {p}") from e
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in {p}: {e}") from e

    if data is None:
        return {}
    if require_mapping and not isinstance(data, dict):
        raise TypeError(
            f"Top-level YAML in {p} must be a mapping, got {type(data).__name__}")
    return data
