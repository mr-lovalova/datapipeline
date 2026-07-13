from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

from datapipeline.utils.load import load_yaml
from datapipeline.config.catalog import (
    AlignedStreamConfig,
    IngestConfig,
    SourceConfig,
    StreamConfig,
    StreamsConfig,
)
from datapipeline.services.project_paths import ingests_dirs, streams_dirs, sources_dirs
from datapipeline.services.constants import (
    SOURCE_ID_KEY,
    STREAM_ID_KEY,
    POSTPROCESS_PATH_KEY,
)
from datapipeline.services.streams.ingest import (
    build_mapper_from_spec,
    build_source_from_spec,
)
from datapipeline.services.streams.aligned import build_combine_stage
from datapipeline.services.streams.validation import (
    stream_feature_id_by,
    stream_partition_by,
    validate_ingest_sources,
    validate_unique_stream_ids,
    validate_stream_configs,
)

from datapipeline.runtime import (
    AlignedRuntimeStream,
    DerivedRuntimeStream,
    IngestRuntimeStream,
    Runtime,
)
from datapipeline.sources.models.source import Source
from datapipeline.config.postprocess import PostprocessConfig
from datapipeline.services.config_refs import resolve_config_refs
from .config import (
    artifacts_root,
    _globals,
    _interpolate,
    _load_by_key,
    _project,
)


_STREAM_CONFIG_ADAPTER: TypeAdapter[StreamConfig] = TypeAdapter(StreamConfig)


def _load_sources_from_dir(
    project_yaml: Path,
    vars_: dict[str, Any],
) -> dict[str, SourceConfig]:
    """Load source files by their declared ids."""
    out: dict[str, SourceConfig] = {}
    source_paths: dict[str, Path] = {}
    for path in _source_yaml_files(project_yaml):
        source = _load_source_yaml(path, project_yaml, vars_)
        source_id = source.id
        if source_id in out:
            raise ValueError(
                f"Duplicate source id '{source_id}' in source files: "
                f"{source_paths[source_id]} and {path}"
            )
        out[source_id] = source
        source_paths[source_id] = path
    return out


def _source_yaml_files(project_yaml: Path) -> list[Path]:
    return _yaml_files_from_roots(sources_dirs(project_yaml))


def _load_source_yaml(
    path: Path,
    project_yaml: Path,
    vars_: dict[str, Any],
) -> SourceConfig:
    data = resolve_config_refs(load_yaml(path), project_yaml=project_yaml)
    data = _interpolate(data, vars_)
    if not data.get(SOURCE_ID_KEY):
        raise ValueError(f"Missing 'id' in source file: {path}")
    return SourceConfig.model_validate(data)


def _load_canonical_streams(
    project_yaml: Path,
    vars_: dict[str, Any],
) -> dict[str, StreamConfig]:
    """Load stream files by their declared ids."""
    out: dict[str, StreamConfig] = {}
    stream_paths: dict[str, Path] = {}
    for path in _stream_yaml_files(project_yaml):
        stream = _load_stream_yaml(path, project_yaml, vars_)
        stream_id = stream.id
        if stream_id in out:
            raise ValueError(
                f"Duplicate stream id '{stream_id}' in stream files: "
                f"{stream_paths[stream_id]} and {path}"
            )
        out[stream_id] = stream
        stream_paths[stream_id] = path
    return out


def _load_canonical_ingests(
    project_yaml: Path,
    vars_: dict[str, Any],
) -> dict[str, IngestConfig]:
    out: dict[str, IngestConfig] = {}
    ingest_paths: dict[str, Path] = {}
    for path in _ingest_yaml_files(project_yaml):
        ingest = _load_ingest_yaml(path, project_yaml, vars_)
        ingest_id = ingest.id
        if ingest_id in out:
            raise ValueError(
                f"Duplicate stream id '{ingest_id}' in ingest files: "
                f"{ingest_paths[ingest_id]} and {path}"
            )
        out[ingest_id] = ingest
        ingest_paths[ingest_id] = path
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


def _load_stream_yaml(
    path: Path,
    project_yaml: Path,
    vars_: dict[str, Any],
) -> StreamConfig:
    data = resolve_config_refs(load_yaml(path), project_yaml=project_yaml)
    data = _interpolate(data, vars_)
    if not data.get(STREAM_ID_KEY):
        raise ValueError(f"Missing 'id' in stream file: {path}")
    return _STREAM_CONFIG_ADAPTER.validate_python(data)


def _load_ingest_yaml(
    path: Path,
    project_yaml: Path,
    vars_: dict[str, Any],
) -> IngestConfig:
    data = resolve_config_refs(load_yaml(path), project_yaml=project_yaml)
    data = _interpolate(data, vars_)
    if not data.get(STREAM_ID_KEY):
        raise ValueError(f"Missing 'id' in ingest file: {path}")
    return IngestConfig.model_validate(data)


def load_streams(project_yaml: Path) -> StreamsConfig:
    vars_ = _globals(project_yaml)
    sources = _load_sources_from_dir(project_yaml, vars_)
    ingests = _load_canonical_ingests(project_yaml, vars_)
    stream_configs = _load_canonical_streams(project_yaml, vars_)
    config = StreamsConfig(
        sources=sources,
        ingests=ingests,
        streams=stream_configs,
    )
    validate_unique_stream_ids(config.ingests, config.streams)
    validate_ingest_sources(config.sources, config.ingests)
    validate_stream_configs(config.ingests, config.streams)
    return config


def init_streams(cfg: StreamsConfig, runtime: Runtime) -> None:
    """Compile typed stream config into prepared runtime streams."""
    runtime.streams.clear()
    ingests = cfg.ingests
    stream_configs = cfg.streams
    sources = {
        alias: build_source_from_spec(spec, project_yaml=runtime.project_yaml)
        for alias, spec in cfg.sources.items()
    }

    for alias, ingest_spec in ingests.items():
        runtime.streams[alias] = _build_ingest_runtime_stream(
            ingest_spec,
            sources,
        )
    for alias, stream_spec in stream_configs.items():
        runtime.streams[alias] = _build_runtime_stream(
            stream_spec,
            ingests,
            stream_configs,
        )


def _build_ingest_runtime_stream(
    spec: IngestConfig,
    sources: dict[str, Source],
) -> IngestRuntimeStream:
    return IngestRuntimeStream(
        source=sources[spec.from_.source],
        mapper=build_mapper_from_spec(spec.map),
        transforms=tuple(spec.record),
        partition_by=spec.partition_by,
        feature_id_by=spec.feature_id_by,
        presorted=spec.ordered_by is not None,
    )


def _build_runtime_stream(
    spec: StreamConfig,
    ingests: dict[str, IngestConfig],
    stream_configs: dict[str, StreamConfig],
) -> DerivedRuntimeStream | AlignedRuntimeStream:
    partition_by = stream_partition_by(ingests, stream_configs, spec.id)
    feature_id_by = stream_feature_id_by(ingests, stream_configs, spec.id)
    if isinstance(spec, AlignedStreamConfig):
        return AlignedRuntimeStream(
            input_streams=spec.input_streams(),
            combine=build_combine_stage(spec),
            transforms=tuple(spec.stream),
            partition_by=partition_by,
            feature_id_by=feature_id_by,
            presorted=spec.ordered_by is not None,
        )
    return DerivedRuntimeStream(
        input_stream=spec.from_.stream,
        mapper=build_mapper_from_spec(spec.map),
        transforms=tuple(spec.stream),
        partition_by=partition_by,
        feature_id_by=feature_id_by,
        presorted=spec.ordered_by is not None,
    )


def bootstrap(project_yaml: Path) -> Runtime:
    """One-call init returning a scoped Runtime.

    Loads streams and postprocess config, prepares streams, and wires artifacts
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
    runtime.postprocess = _load_postprocess_config(project_yaml)
    return runtime


def _attach_project_config(runtime: Runtime, project_yaml: Path) -> None:
    proj = _project(project_yaml)
    runtime.split = proj.split
    runtime.split_labels = ()


def _load_postprocess_config(project_yaml: Path) -> PostprocessConfig:
    post_doc = _load_by_key(
        project_yaml,
        POSTPROCESS_PATH_KEY,
        require_mapping=False,
    )
    post_doc = _interpolate(post_doc, _globals(project_yaml))
    if post_doc is None:
        return PostprocessConfig()
    return PostprocessConfig.model_validate(post_doc)
