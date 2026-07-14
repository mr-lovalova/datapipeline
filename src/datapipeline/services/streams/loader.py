from pathlib import Path

from pydantic import TypeAdapter

from datapipeline.config.catalog import (
    IngestConfig,
    SourceConfig,
    StreamConfig,
    StreamsConfig,
)
from datapipeline.services.config_refs import (
    interpolate_config_vars,
    resolve_config_refs,
)
from datapipeline.services.config_inventory import pipeline_yaml_files
from datapipeline.services.constants import SOURCE_ID_KEY, STREAM_ID_KEY
from datapipeline.services.definitions import ProjectManifest
from datapipeline.services.streams.validation import (
    validate_ingest_sources,
    validate_stream_configs,
    validate_unique_stream_ids,
)
from datapipeline.utils.load import YamlDocument, read_yaml_document


_STREAM_CONFIG_ADAPTER: TypeAdapter[StreamConfig] = TypeAdapter(StreamConfig)


def _source_documents(project: ProjectManifest) -> tuple[YamlDocument, ...]:
    documents: list[YamlDocument] = []
    for root in project.source_dirs:
        try:
            paths = pipeline_yaml_files(root)
        except (FileNotFoundError, NotADirectoryError) as exc:
            raise FileNotFoundError(f"sources dir not found: {root}") from exc
        documents.extend(read_yaml_document(path) for path in paths)
    return tuple(documents)


def _ingest_documents(project: ProjectManifest) -> tuple[YamlDocument, ...]:
    documents: list[YamlDocument] = []
    for root in project.ingest_dirs:
        try:
            paths = pipeline_yaml_files(root)
        except (FileNotFoundError, NotADirectoryError) as exc:
            raise FileNotFoundError(f"ingests dir not found: {root}") from exc
        documents.extend(read_yaml_document(path) for path in paths)
    return tuple(documents)


def _stream_documents(project: ProjectManifest) -> tuple[YamlDocument, ...]:
    documents: list[YamlDocument] = []
    for root in project.stream_dirs:
        try:
            paths = pipeline_yaml_files(root)
        except (FileNotFoundError, NotADirectoryError) as exc:
            raise FileNotFoundError(f"streams dir not found: {root}") from exc
        documents.extend(read_yaml_document(path) for path in paths)
    return tuple(documents)


def stream_documents(
    project: ProjectManifest,
) -> tuple[
    tuple[YamlDocument, ...],
    tuple[YamlDocument, ...],
    tuple[YamlDocument, ...],
]:
    return (
        _source_documents(project),
        _ingest_documents(project),
        _stream_documents(project),
    )


def _source_from_document(
    project: ProjectManifest,
    document: YamlDocument,
) -> SourceConfig:
    data = resolve_config_refs(
        document.data,
        project_yaml=project.path,
        env=project.environment,
    )
    data = interpolate_config_vars(data, project.variables)
    if not data.get(SOURCE_ID_KEY):
        raise ValueError(f"Missing 'id' in source file: {document.path}")
    return SourceConfig.model_validate(data)


def _ingest_from_document(
    project: ProjectManifest,
    document: YamlDocument,
) -> IngestConfig:
    data = resolve_config_refs(
        document.data,
        project_yaml=project.path,
        env=project.environment,
    )
    data = interpolate_config_vars(data, project.variables)
    if not data.get(STREAM_ID_KEY):
        raise ValueError(f"Missing 'id' in ingest file: {document.path}")
    return IngestConfig.model_validate(data)


def _stream_from_document(
    project: ProjectManifest,
    document: YamlDocument,
) -> StreamConfig:
    data = resolve_config_refs(
        document.data,
        project_yaml=project.path,
        env=project.environment,
    )
    data = interpolate_config_vars(data, project.variables)
    if not data.get(STREAM_ID_KEY):
        raise ValueError(f"Missing 'id' in stream file: {document.path}")
    return _STREAM_CONFIG_ADAPTER.validate_python(data)


def streams_from_documents(
    project: ProjectManifest,
    source_documents: tuple[YamlDocument, ...],
    ingest_documents: tuple[YamlDocument, ...],
    stream_documents: tuple[YamlDocument, ...],
) -> StreamsConfig:
    sources: dict[str, SourceConfig] = {}
    source_paths: dict[str, Path] = {}
    for document in source_documents:
        source = _source_from_document(project, document)
        if source.id in sources:
            raise ValueError(
                f"Duplicate source id '{source.id}' in source files: "
                f"{source_paths[source.id]} and {document.path}"
            )
        sources[source.id] = source
        source_paths[source.id] = document.path

    ingests: dict[str, IngestConfig] = {}
    ingest_paths: dict[str, Path] = {}
    for document in ingest_documents:
        ingest = _ingest_from_document(project, document)
        if ingest.id in ingests:
            raise ValueError(
                f"Duplicate stream id '{ingest.id}' in ingest files: "
                f"{ingest_paths[ingest.id]} and {document.path}"
            )
        ingests[ingest.id] = ingest
        ingest_paths[ingest.id] = document.path

    streams: dict[str, StreamConfig] = {}
    stream_paths: dict[str, Path] = {}
    for document in stream_documents:
        stream = _stream_from_document(project, document)
        if stream.id in streams:
            raise ValueError(
                f"Duplicate stream id '{stream.id}' in stream files: "
                f"{stream_paths[stream.id]} and {document.path}"
            )
        streams[stream.id] = stream
        stream_paths[stream.id] = document.path

    config = StreamsConfig(sources=sources, ingests=ingests, streams=streams)
    validate_unique_stream_ids(config.ingests, config.streams)
    validate_ingest_sources(config.sources, config.ingests)
    validate_stream_configs(config.ingests, config.streams)
    return config


def load_streams(project: ProjectManifest) -> StreamsConfig:
    sources, ingests, streams = stream_documents(project)
    return streams_from_documents(project, sources, ingests, streams)
