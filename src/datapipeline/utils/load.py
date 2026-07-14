import importlib.metadata as md
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True, slots=True)
class YamlDocument:
    path: Path
    content: bytes
    data: Any


@lru_cache
def load_ep(group: str, name: str) -> Any:
    eps = md.entry_points().select(group=group, name=name)
    if not eps:
        available = ", ".join(
            sorted(ep.name for ep in md.entry_points().select(group=group))
        )
        raise ValueError(
            f"No entry point '{name}' in '{group}'. Available: {available or '(none)'}"
        )
    if len(eps) > 1:
        mods = ", ".join(ep.value for ep in eps)
        raise ValueError(f"Ambiguous entry point '{name}' in '{group}': {mods}")
    return next(iter(eps)).load()


def read_yaml_document(path: Path, require_mapping: bool = True) -> YamlDocument:
    try:
        content = path.read_bytes()
        data = yaml.safe_load(content.decode("utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"YAML file not found: {path}") from exc
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError(f"Invalid YAML in {path}: {exc}") from exc

    if data is None:
        data = {}
    if require_mapping and not isinstance(data, dict):
        raise TypeError(
            f"Top-level YAML in {path} must be a mapping, got {type(data).__name__}"
        )
    return YamlDocument(path=path.resolve(), content=content, data=data)


def load_yaml(path: Path, require_mapping: bool = True) -> Any:
    return read_yaml_document(path, require_mapping=require_mapping).data
