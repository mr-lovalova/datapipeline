from pathlib import Path
from typing import Any

from datapipeline.utils.load import load_yaml
from datapipeline.config.catalog import StreamConfig, StreamsConfig
from datapipeline.services.project_paths import streams_dir, sources_dir
from datapipeline.build.state import load_build_state
from datapipeline.services.constants import (
    PARSER_KEY,
    LOADER_KEY,
    SOURCE_ID_KEY,
    ENTRYPOINT_KEY,
    STREAM_ID_KEY,
    STREAM_FROM_KEY,
    STREAM_MAP_KEY,
    STREAM_CADENCE_KEY,
    LEGACY_STREAM_KIND_KEY,
    POSTPROCESS_PATH_KEY,
    POSTPROCESS_TRANSFORMS,
)
from datapipeline.services.streams.ingest import (
    build_mapper_from_spec,
    build_source_from_spec,
)
from datapipeline.services.streams.joined import build_joined_stream
from datapipeline.services.streams.manual import build_manual_stream
from datapipeline.services.streams.validation import (
    stream_partition_by,
    validate_stream_configs,
)

from datapipeline.runtime import Runtime
from datapipeline.config.postprocess import PostprocessConfig
from datapipeline.services.config_refs import resolve_config_refs
from .config import (
    artifacts_root,
    build_state_path,
    _globals,
    _interpolate,
    _load_by_key,
    _project,
)


SRC_PARSER_KEY = PARSER_KEY
SRC_LOADER_KEY = LOADER_KEY


def _load_sources_from_dir(project_yaml: Path, vars_: dict[str, Any]) -> dict:
    """Aggregate per-source YAML files into a raw-sources mapping.

    Scans for YAML files under the sources directory (recursing through
    subfolders). Expects each file to define a single source with top-level
    'parser' and 'loader' keys. The top-level 'id' inside the file becomes the
    runtime alias.
    """
    out: dict[str, dict] = {}
    for path in _source_yaml_files(project_yaml):
        source_doc = _load_source_yaml(path, project_yaml, vars_)
        if source_doc is None:
            continue
        out[source_doc[SOURCE_ID_KEY]] = source_doc
    return out


def _source_yaml_files(project_yaml: Path) -> list[Path]:
    return _yaml_files(sources_dir(project_yaml))


def _load_source_yaml(
    path: Path,
    project_yaml: Path,
    vars_: dict[str, Any],
) -> dict[str, Any] | None:
    data = resolve_config_refs(load_yaml(path), project_yaml=project_yaml)
    if not isinstance(data, dict) or not _is_source_yaml(data):
        return None
    if not data.get(SOURCE_ID_KEY):
        src_dir = sources_dir(project_yaml)
        raise ValueError(f"Missing 'id' in source file: {path.relative_to(src_dir)}")
    source_doc = _interpolate(data, vars_)
    return source_doc


def _is_source_yaml(data: dict[str, Any]) -> bool:
    return (
        isinstance(data.get(SRC_PARSER_KEY), dict)
        and isinstance(data.get(SRC_LOADER_KEY), dict)
    )


def _load_canonical_streams(project_yaml: Path, vars_: dict[str, Any]) -> dict:
    """Aggregate canonical stream specs from streams_dir (supports subfolders).

    Recursively scans for *.yml|*.yaml under the configured streams dir.
    Stream alias is derived from the relative path with '/' replaced by '.'
    and extension removed, e.g. 'metobs/precip.yaml' -> 'metobs.precip'.
    """
    out: dict[str, dict] = {}
    for path in _stream_yaml_files(project_yaml):
        data = _load_stream_yaml(path, project_yaml)
        if data is None:
            continue
        alias = data[STREAM_ID_KEY]
        out[alias] = _interpolate_stream_yaml(data, vars_)
    return out


def _stream_yaml_files(project_yaml: Path) -> list[Path]:
    return _yaml_files(streams_dir(project_yaml))


def _yaml_files(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted(
        (path for path in root.rglob("*.y*ml") if path.is_file()),
        key=lambda path: path.relative_to(root).as_posix(),
    )


def _load_stream_yaml(path: Path, project_yaml: Path) -> dict[str, Any] | None:
    data = resolve_config_refs(load_yaml(path), project_yaml=project_yaml)
    if not isinstance(data, dict):
        return None
    if LEGACY_STREAM_KIND_KEY in data:
        raise ValueError(
            "Stream config field 'kind' is no longer supported; use 'from'."
        )
    if STREAM_ID_KEY not in data or STREAM_FROM_KEY not in data:
        return None
    _normalize_stream_map(data)
    return data


def _normalize_stream_map(data: dict[str, Any]) -> None:
    mapper = data.get(STREAM_MAP_KEY)
    if not isinstance(mapper, dict) or ENTRYPOINT_KEY not in mapper:
        data[STREAM_MAP_KEY] = None


def _interpolate_stream_yaml(
    data: dict[str, Any],
    vars_: dict[str, Any],
) -> dict[str, Any]:
    local_vars = dict(vars_)
    cadence_expr = data.get(STREAM_CADENCE_KEY)
    if cadence_expr is not None:
        local_vars[STREAM_CADENCE_KEY] = _interpolate(cadence_expr, vars_)
    return _interpolate(data, local_vars)


def load_streams(project_yaml: Path) -> StreamsConfig:
    vars_ = _globals(project_yaml)
    raw = _load_sources_from_dir(project_yaml, vars_)
    stream_configs = _load_canonical_streams(project_yaml, vars_)
    return StreamsConfig(raw=raw, streams=stream_configs)


def init_streams(cfg: StreamsConfig, runtime: Runtime) -> None:
    """Compile typed streams config into runtime registries."""
    regs = runtime.registries
    regs.clear_all()
    stream_configs = cfg.streams or {}
    validate_stream_configs(stream_configs)

    for alias, spec in stream_configs.items():
        _register_stream_policy(runtime, alias, spec, stream_configs)

    for alias, spec in (cfg.raw or {}).items():
        regs.sources.register(
            alias,
            build_source_from_spec(spec, project_yaml=runtime.project_yaml),
        )

    for alias, spec in stream_configs.items():
        _register_stream_source(runtime, alias, spec)


def _register_stream_policy(
    runtime: Runtime,
    alias: str,
    spec: StreamConfig,
    stream_configs: dict[str, StreamConfig],
) -> None:
    regs = runtime.registries
    regs.stream_operations.register(alias, spec.stream)
    regs.debug_operations.register(alias, spec.debug)
    regs.partition_by.register(alias, stream_partition_by(stream_configs, spec))
    regs.sort_batch_size.register(alias, spec.sort_batch_size)
    regs.record_operations.register(alias, spec.record)


def _register_stream_source(
    runtime: Runtime,
    alias: str,
    spec: StreamConfig,
) -> None:
    regs = runtime.registries
    if spec.maps_streams:
        regs.stream_sources.register(alias, build_manual_stream(alias, spec, runtime))
        regs.mappers.register(alias, build_mapper_from_spec(None))
        return
    if spec.joins_streams:
        regs.stream_sources.register(alias, build_joined_stream(alias, spec, runtime))
        regs.mappers.register(
            alias,
            build_mapper_from_spec(spec.map, runtime=runtime, row_mapper=True),
        )
        return

    regs.mappers.register(alias, build_mapper_from_spec(spec.map))
    regs.stream_sources.register(alias, regs.sources.get(spec.from_.source))


def bootstrap(project_yaml: Path) -> Runtime:
    """One-call init returning a scoped Runtime.

    Loads streams and postprocess config, fills registries, and wires artifacts
    under a per-project runtime instance.
    """
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root(project_yaml),
    )
    _attach_project_config(runtime, project_yaml)
    init_streams(load_streams(project_yaml), runtime)
    _register_postprocesses(runtime, project_yaml)
    _register_build_artifacts(runtime, project_yaml)
    return runtime


def _attach_project_config(runtime: Runtime, project_yaml: Path) -> None:
    proj = _project(project_yaml)
    runtime.split = proj.globals.split
    runtime.run = None
    runtime.split_keep = getattr(runtime.split, "keep", None)


def _register_postprocesses(runtime: Runtime, project_yaml: Path) -> None:
    transforms = _load_postprocess_transforms(project_yaml)
    runtime.registries.postprocesses.register(POSTPROCESS_TRANSFORMS, transforms)


def _load_postprocess_transforms(project_yaml: Path):
    post_doc = _load_by_key(
        project_yaml,
        POSTPROCESS_PATH_KEY,
        require_mapping=False,
    )
    post_doc = _interpolate(post_doc, _globals(project_yaml))
    if post_doc is None:
        return None
    return PostprocessConfig.model_validate(post_doc).root


def _register_build_artifacts(runtime: Runtime, project_yaml: Path) -> None:
    state = load_build_state(build_state_path(project_yaml))
    if state:
        for key, info in state.artifacts.items():
            runtime.artifacts.register(
                key,
                relative_path=info.relative_path,
                meta=info.meta,
            )
