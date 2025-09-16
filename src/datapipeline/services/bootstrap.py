from __future__ import annotations

from pathlib import Path
from typing import Mapping
import re
from datetime import datetime, timezone

from datapipeline.utils.load import load_yaml
from datapipeline.config.catalog import StreamsConfig
from datapipeline.config.project import ProjectConfig
from datapipeline.services.project_paths import streams_dir, sources_dir
from datapipeline.services.constants import (
    PARSER_KEY,
    LOADER_KEY,
    SOURCE_KEY,
    MAPPER_KEY,
    ENTRYPOINT_KEY,
)
from datapipeline.streams.raw import register_source
from datapipeline.streams.canonical import register_stream
from datapipeline.services.factories import (
    build_source_from_spec,
    build_mapper_from_spec,
)

SRC_PARSER_KEY = PARSER_KEY
SRC_LOADER_KEY = LOADER_KEY


def _project(project_yaml: Path) -> ProjectConfig:
    """Load and validate project.yaml."""
    data = load_yaml(project_yaml)
    return ProjectConfig.model_validate(data)


def _paths(project_yaml: Path) -> Mapping[str, str]:
    proj = _project(project_yaml)
    return proj.paths.model_dump()


def _load_by_key(project_yaml: Path, key: str) -> dict:
    """Load a YAML document referenced by project.paths[key]. (Legacy)"""
    p = _paths(project_yaml).get(key)
    if not p:
        raise FileNotFoundError(f"project.paths must include '{key}'.")
    return load_yaml(Path(p))


def _globals(project_yaml: Path) -> dict[str, str]:
    """Return project-level globals for interpolation.

    If a value is a datetime, normalize to strict UTC Z-format string so
    downstream components expecting ISO Z will work predictably.
    Otherwise, coerce to string.
    """
    proj = _project(project_yaml)
    g = proj.globals.model_dump()
    out: dict[str, str] = {}
    for k, v in g.items():
        if isinstance(v, datetime):
            v = v.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        out[str(k)] = str(v)
    return out


_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _interpolate(obj, vars_: dict[str, str]):
    """Recursively substitute ${var} in strings using vars_ map.

    Minimal behavior: if a key is missing, leave placeholder as-is.
    """
    if isinstance(obj, dict):
        return {k: _interpolate(v, vars_) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate(v, vars_) for v in obj]
    if isinstance(obj, str):
        def repl(m):
            key = m.group(1)
            return vars_.get(key, m.group(0))
        return _VAR_RE.sub(repl, obj)
    return obj


def _load_sources_from_dir(project_yaml: Path, vars_: dict[str, str]) -> dict:
    """Aggregate per-source YAML files into a raw-sources mapping.

    Expects each file to define a single source with top-level 'parser' and
    'loader' keys. The source alias is inferred from the filename (without
    extension).
    """
    import os
    src_dir = sources_dir(project_yaml)
    if not src_dir.exists() or not src_dir.is_dir():
        return {}
    out: dict[str, dict] = {}
    for fname in sorted(os.listdir(src_dir)):
        if not (fname.endswith(".yaml") or fname.endswith(".yml")):
            continue
        data = load_yaml(src_dir / fname)
        if not isinstance(data, dict):
            continue
        # Top-level parser/loader; alias inferred from filename
        if isinstance(data.get(SRC_PARSER_KEY), dict) and isinstance(data.get(SRC_LOADER_KEY), dict):
            alias = Path(fname).stem
            out[alias] = _interpolate(data, vars_)
            continue
    return out


def _load_canonical_streams(project_yaml: Path, vars_: dict[str, str]) -> dict:
    """Aggregate canonical stream specs from streams_dir (supports subfolders).

    Recursively scans for *.yml|*.yaml under the configured streams dir.
    Stream alias is derived from the relative path with '/' replaced by '.'
    and extension removed, e.g. 'metobs/precip.yaml' → 'metobs.precip'.
    """
    out: dict[str, dict] = {}
    sdir = streams_dir(project_yaml)
    if not sdir.exists() or not sdir.is_dir():
        return {}
    for p in sorted(sdir.rglob("*.y*ml")):
        if not p.is_file():
            continue
        data = load_yaml(p)
        if isinstance(data, dict) and (SOURCE_KEY in data):
            m = data.get(MAPPER_KEY)
            if (not isinstance(m, dict)) or (ENTRYPOINT_KEY not in (m or {})):
                data[MAPPER_KEY] = None
            rel = p.relative_to(sdir)
            alias = rel.with_suffix("").as_posix().replace("/", ".")
            out[alias] = _interpolate(data, vars_)
    return out


def load_streams(project_yaml: Path) -> StreamsConfig:
    vars_ = _globals(project_yaml)
    raw = _load_sources_from_dir(project_yaml, vars_)
    canonical = _load_canonical_streams(project_yaml, vars_)
    return StreamsConfig(raw=raw, canonical=canonical)


def init_streams(cfg: StreamsConfig) -> None:
    """Compile typed streams config into runtime registries."""
    for alias, spec in (cfg.raw or {}).items():
        register_source(alias, build_source_from_spec(spec))
    for alias, spec in (cfg.canonical or {}).items():
        fn, margs = build_mapper_from_spec(spec.mapper)  # None -> identity
        register_stream(alias, source_alias=spec.source,
                        mapper=fn, mapper_args=margs)


def bootstrap(project_yaml: Path) -> StreamsConfig:
    """One-call init: load streams.yaml and register raw/canonical streams."""
    streams = load_streams(project_yaml)
    init_streams(streams)
    return streams
