import importlib.metadata as md
from functools import lru_cache
import yaml
from pathlib import Path


@lru_cache
def load_ep(group: str, name: str):
    eps = md.entry_points().select(group=group, name=name)
    if not eps:
        available = ", ".join(
            sorted(ep.name for ep in md.entry_points().select(group=group)))
        raise ValueError(
            f"No entry point '{name}' in '{group}'. Available: {available or '(none)'}")
    if len(eps) > 1:
        mods = ", ".join(f"{ep.module}:{ep.attr}" for ep in eps)
        raise ValueError(
            f"Ambiguous entry point '{name}' in '{group}': {mods}")
    # EntryPoints in newer Python versions are mapping-like; avoid integer indexing
    ep = next(iter(eps))
    return ep.load()


def load_yaml(p: Path, *, require_mapping: bool = True):
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
