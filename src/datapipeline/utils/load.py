import importlib
import importlib.metadata as md
from functools import lru_cache
from pathlib import Path

import yaml


_BUILTIN_EP_FALLBACKS: dict[tuple[str, str], str] = {
    # Vector postprocess transforms
    ("datapipeline.transforms.vector", "drop"): "datapipeline.transforms.vector:VectorDropTransform",
    ("datapipeline.transforms.vector", "fill"): "datapipeline.transforms.vector:VectorFillTransform",
    ("datapipeline.transforms.vector", "replace"): "datapipeline.transforms.vector:VectorReplaceTransform",
}


def _load_from_spec(spec: str):
    module_name, _, attr = spec.partition(":")
    if not module_name or not attr:
        raise ValueError(f"Invalid fallback entry point spec: {spec!r}")
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attr)
    except AttributeError as exc:
        raise ImportError(f"Fallback entry point attribute {attr!r} not found in {module_name!r}") from exc


@lru_cache
def load_ep(group: str, name: str):
    eps = md.entry_points().select(group=group, name=name)
    if not eps:
        # Fallback to built-in registry to support running from a source checkout
        # without requiring the package to be reinstalled after pyproject changes.
        key = (group, name)
        spec = _BUILTIN_EP_FALLBACKS.get(key)
        if spec:
            return _load_from_spec(spec)
        available_eps = md.entry_points().select(group=group)
        available_fallbacks = [n for (g, n), _ in _BUILTIN_EP_FALLBACKS.items() if g == group]
        available = ", ".join(sorted({ep.name for ep in available_eps} | set(available_fallbacks)))
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
