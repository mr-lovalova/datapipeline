from pathlib import Path
from typing import Any

from datapipeline.utils.load import load_yaml
from datapipeline.config.catalog import IngestConfig, StreamConfig, StreamsConfig
from datapipeline.services.project_paths import ingests_dirs, streams_dirs, sources_dirs
from datapipeline.services.constants import (
    PARSER_KEY,
    LOADER_KEY,
    SOURCE_ID_KEY,
    STREAM_ID_KEY,
    STREAM_FROM_KEY,
    STREAM_MAP_KEY,
    STREAM_CADENCE_KEY,
    STREAM_KIND_KEY,
    POSTPROCESS_PATH_KEY,
    POSTPROCESS_TRANSFORMS,
)
from datapipeline.services.streams.ingest import (
    build_mapper_from_spec,
    build_source_from_spec,
)
from datapipeline.services.streams.joined import build_joined_stream
from datapipeline.services.streams.manual import build_manual_stream
from datapipeline.services.streams.simple import build_prepared_stream_ref
from datapipeline.services.streams.validation import (
    stream_partition_by,
    validate_unique_stream_ids,
    validate_stream_configs,
)

from datapipeline.runtime import Runtime, StreamRuntimeSpec
from datapipeline.config.postprocess import PostprocessConfig
from datapipeline.services.config_refs import resolve_config_refs
from .config import (
    artifacts_root,
    _globals,
    _interpolate,
    _load_by_key,
    _project,
)


def _load_sources_from_dir(project_yaml: Path, vars_: dict[str, Any]) -> dict:
    """Aggregate per-source YAML files into a raw-sources mapping.

    Scans for YAML files under the sources directory (recursing through
    subfolders). Expects each file to define a single source with top-level
    'parser' and 'loader' keys. The top-level 'id' inside the file becomes the
    runtime alias.
    """
    out: dict[str, dict] = {}
    source_paths: dict[str, Path] = {}
    for path in _source_yaml_files(project_yaml):
        source_doc = _load_source_yaml(path, project_yaml, vars_)
        if source_doc is None:
            continue
        source_id = source_doc[SOURCE_ID_KEY]
        if source_id in out:
            raise ValueError(
                f"Duplicate source id '{source_id}' in source files: "
                f"{source_paths[source_id]} and {path}"
            )
        out[source_id] = source_doc
        source_paths[source_id] = path
    return out


def _source_yaml_files(project_yaml: Path) -> list[Path]:
    return _yaml_files_from_roots(sources_dirs(project_yaml))


def _load_source_yaml(
    path: Path,
    project_yaml: Path,
    vars_: dict[str, Any],
) -> dict[str, Any] | None:
    data = resolve_config_refs(load_yaml(path), project_yaml=project_yaml)
    if not isinstance(data, dict) or not _is_source_yaml(data):
        return None
    if not data.get(SOURCE_ID_KEY):
        raise ValueError(f"Missing 'id' in source file: {path}")
    source_doc = _interpolate(data, vars_)
    return source_doc


def _is_source_yaml(data: dict[str, Any]) -> bool:
    return isinstance(data.get(PARSER_KEY), dict) and isinstance(
        data.get(LOADER_KEY), dict
    )


def _load_canonical_streams(project_yaml: Path, vars_: dict[str, Any]) -> dict:
    """Aggregate canonical stream specs from streams_dir (supports subfolders).

    Recursively scans for *.yml|*.yaml under the configured streams dir.
    Stream alias is derived from the relative path with '/' replaced by '.'
    and extension removed, e.g. 'metobs/precip.yaml' -> 'metobs.precip'.
    """
    out: dict[str, dict] = {}
    stream_paths: dict[str, Path] = {}
    for path in _stream_yaml_files(project_yaml):
        data = _load_stream_yaml(path, project_yaml)
        if data is None:
            continue
        alias = data[STREAM_ID_KEY]
        if alias in out:
            raise ValueError(
                f"Duplicate stream id '{alias}' in stream files: "
                f"{stream_paths[alias]} and {path}"
            )
        out[alias] = _interpolate_stream_yaml(data, vars_)
        stream_paths[alias] = path
    return out


def _load_canonical_ingests(project_yaml: Path, vars_: dict[str, Any]) -> dict:
    out: dict[str, dict] = {}
    ingest_paths: dict[str, Path] = {}
    for path in _ingest_yaml_files(project_yaml):
        data = _load_ingest_yaml(path, project_yaml)
        if data is None:
            continue
        alias = data[STREAM_ID_KEY]
        if alias in out:
            raise ValueError(
                f"Duplicate stream id '{alias}' in ingest files: "
                f"{ingest_paths[alias]} and {path}"
            )
        out[alias] = _interpolate_stream_yaml(data, vars_)
        ingest_paths[alias] = path
    return out


def _ingest_yaml_files(project_yaml: Path) -> list[Path]:
    return _yaml_files_from_roots(ingests_dirs(project_yaml))


def _stream_yaml_files(project_yaml: Path) -> list[Path]:
    return _yaml_files_from_roots(streams_dirs(project_yaml))


def _yaml_files_from_roots(roots: list[Path]) -> list[Path]:
    out: list[Path] = []
    for root in roots:
        out.extend(_yaml_files(root))
    return out


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
    if STREAM_KIND_KEY in data:
        raise ValueError(
            "Stream config field 'kind' is no longer supported; use 'from'."
        )
    if STREAM_ID_KEY not in data or STREAM_FROM_KEY not in data:
        return None
    if "record" in data:
        raise ValueError("Stream configs cannot define 'record'; use ingests.")
    _normalize_stream_map(data)
    return data


def _load_ingest_yaml(path: Path, project_yaml: Path) -> dict[str, Any] | None:
    data = resolve_config_refs(load_yaml(path), project_yaml=project_yaml)
    if not isinstance(data, dict):
        return None
    if STREAM_KIND_KEY in data:
        raise ValueError(
            "Ingest config field 'kind' is no longer supported; use 'from'."
        )
    if STREAM_ID_KEY not in data or STREAM_FROM_KEY not in data:
        return None
    if "stream" in data:
        raise ValueError("Ingest configs cannot define 'stream'; use streams.")
    return data


def _normalize_stream_map(data: dict[str, Any]) -> None:
    if STREAM_MAP_KEY not in data:
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
    ingests = _load_canonical_ingests(project_yaml, vars_)
    stream_configs = _load_canonical_streams(project_yaml, vars_)
    return StreamsConfig(raw=raw, ingests=ingests, streams=stream_configs)


def init_streams(cfg: StreamsConfig, runtime: Runtime) -> None:
    """Compile typed streams config into runtime registries."""
    regs = runtime.registries
    regs.clear_all()
    ingests = cfg.ingests or {}
    stream_configs = cfg.streams or {}
    validate_unique_stream_ids(ingests, stream_configs)
    validate_stream_configs(ingests, stream_configs)

    for alias, ingest_spec in ingests.items():
        _register_ingest_policy(runtime, alias, ingest_spec)
    for alias, stream_spec in stream_configs.items():
        _register_stream_policy(
            runtime,
            alias,
            stream_spec,
            ingests,
            stream_configs,
        )

    for alias, source_spec in (cfg.raw or {}).items():
        regs.sources.register(
            alias,
            build_source_from_spec(source_spec, project_yaml=runtime.project_yaml),
        )

    for alias, ingest_spec in ingests.items():
        _register_ingest_source(runtime, alias, ingest_spec)
    for alias, stream_spec in stream_configs.items():
        _register_stream_source(runtime, alias, stream_spec)


def _register_ingest_policy(
    runtime: Runtime,
    alias: str,
    spec: IngestConfig,
) -> None:
    regs = runtime.registries
    regs.record_operations.register(alias, spec.record)
    regs.stream_specs.register(alias, StreamRuntimeSpec(pipeline="ingest"))
    regs.stream_operations.register(alias, [])
    regs.debug_operations.register(alias, [])
    regs.partition_by.register(alias, spec.partition_by)
    regs.feature_id_by.register(alias, spec.feature_id_by)
    regs.ordered_by.register(alias, spec.ordered_by)
    regs.sort_batch_size.register(alias, spec.sort_batch_size)


def _register_stream_policy(
    runtime: Runtime,
    alias: str,
    spec: StreamConfig,
    ingests: dict[str, IngestConfig],
    stream_configs: dict[str, StreamConfig],
) -> None:
    regs = runtime.registries
    regs.stream_specs.register(alias, StreamRuntimeSpec(pipeline="stream"))
    regs.stream_operations.register(alias, spec.stream)
    regs.debug_operations.register(alias, spec.debug)
    regs.partition_by.register(
        alias, stream_partition_by(ingests, stream_configs, spec)
    )
    regs.feature_id_by.register(alias, spec.feature_id_by)
    regs.ordered_by.register(alias, spec.ordered_by)
    regs.sort_batch_size.register(alias, spec.sort_batch_size)
    regs.record_operations.register(alias, [])


def _register_ingest_source(
    runtime: Runtime,
    alias: str,
    spec: IngestConfig,
) -> None:
    regs = runtime.registries
    regs.mappers.register(alias, build_mapper_from_spec(spec.map))
    regs.stream_sources.register(alias, regs.sources.get(spec.from_.source))


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
    if spec.from_.stream is None:
        raise ValueError(f"Stream '{alias}' requires from.stream")
    regs.stream_sources.register(
        alias,
        build_prepared_stream_ref(spec.from_.stream, runtime),
    )


def bootstrap(project_yaml: Path) -> Runtime:
    """One-call init returning a scoped Runtime.

    Loads streams and postprocess config, fills registries, and wires artifacts
    under a per-project runtime instance.
    """
    from datapipeline.artifacts.hydration import hydrate_runtime_artifacts_for_project

    runtime = bootstrap_build_runtime(project_yaml)
    hydrate_runtime_artifacts_for_project(runtime, project_yaml)
    return runtime


def bootstrap_build_runtime(project_yaml: Path) -> Runtime:
    """Initialize project services without hydrating cached artifacts."""
    runtime = Runtime(
        project_yaml=project_yaml,
        artifacts_root=artifacts_root(project_yaml),
    )
    _attach_project_config(runtime, project_yaml)
    init_streams(load_streams(project_yaml), runtime)
    _register_postprocesses(runtime, project_yaml)
    return runtime


def _attach_project_config(runtime: Runtime, project_yaml: Path) -> None:
    proj = _project(project_yaml)
    runtime.split = proj.split
    runtime.run = None


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
