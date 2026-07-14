import glob
import hashlib
import json
import stat
from collections.abc import Iterable, Mapping
from pathlib import Path

from datapipeline.config.catalog import (
    CoreIoLoaderConfig,
    FsSourceArgs,
    SourceConfig,
    StreamsConfig,
)
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.tasks import (
    ArtifactTask,
    MetadataTask,
    ScalerTask,
    SchemaTask,
    StatsTask,
    TicksTask,
    VectorInputsTask,
    OperationTask,
)
from datapipeline.artifacts.planning import build_artifact_graph
from datapipeline.services.config_refs import (
    collect_config_ref_keys,
)
from datapipeline.services.definitions import (
    ArtifactHashes,
    PipelineDocuments,
    ProjectManifest,
)
from datapipeline.utils.load import YamlDocument


# Increment when Jerry's core artifact semantics change without a config change.
ARTIFACT_CACHE_VERSION = 2


def _normalized_label(path: Path, base_dir: Path) -> str:
    try:
        return str(path.resolve().relative_to(base_dir))
    except ValueError:
        return str(path.resolve())


def _hash_document(
    hasher,
    document: YamlDocument,
    base_dir: Path,
) -> None:
    hasher.update(_normalized_label(document.path, base_dir).encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(document.content)
    hasher.update(b"\0")


def _all_documents(
    project: ProjectManifest,
    documents: PipelineDocuments,
) -> tuple[YamlDocument, ...]:
    return (
        project.document,
        documents.dataset,
        *documents.operations,
        *documents.ingests,
        *documents.sources,
        *documents.streams,
    )


def _hash_env_refs(
    hasher,
    project: ProjectManifest,
    documents: PipelineDocuments,
) -> None:
    env_ref_names: set[str] = set()
    for document in _all_documents(project, documents):
        env_ref_names.update(
            key for _scheme, key in collect_config_ref_keys(document.data, scheme="env")
        )

    for name in sorted(env_ref_names):
        hasher.update(f"[env]{name}".encode("utf-8"))
        hasher.update(b"\0")
        value = project.environment.get(name)
        hasher.update(b"[missing]" if value is None else value.encode("utf-8"))
        hasher.update(b"\0")


def _source_label(path: Path, base_dir: Path) -> str:
    try:
        return str(path.relative_to(base_dir))
    except ValueError:
        return str(path)


def _hash_source_file(hasher, path: Path, base_dir: Path) -> None:
    try:
        metadata = path.stat()
    except FileNotFoundError:
        state = "missing"
    else:
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"Source input is not a regular file: {path}")
        state = f"file:{metadata.st_size}:{metadata.st_mtime_ns}:{metadata.st_ctime_ns}"
    snapshot = (
        f"{_source_label(path, base_dir)}\0"
        f"{_normalized_label(path, base_dir)}\0{state}\0"
    )
    hasher.update(snapshot.encode("utf-8"))


def _hash_source_pattern(
    hasher,
    pattern: Path,
    expands_glob: bool,
    base_dir: Path,
) -> None:
    hasher.update(f"[source]{_source_label(pattern, base_dir)}\0".encode("utf-8"))
    if expands_glob:
        matches = [Path(match) for match in sorted(glob.glob(str(pattern)))]
        if not matches:
            hasher.update(b"[no-matches]")
            return
    else:
        matches = [pattern]
    for path in matches:
        _hash_source_file(hasher, path, base_dir)


def _project_source_path(project: ProjectManifest, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return project.path.parent / path


def _source_input_patterns(
    source: SourceConfig,
    project: ProjectManifest,
) -> Iterable[tuple[Path, bool]]:
    loader = source.loader
    if isinstance(loader, CoreIoLoaderConfig) and isinstance(loader.args, FsSourceArgs):
        yield (
            _project_source_path(project, loader.args.path),
            glob.has_magic(loader.args.path),
        )
    if source.inputs is not None:
        for raw_path in source.inputs.files:
            yield _project_source_path(project, raw_path), glob.has_magic(raw_path)


def _hash_source_inputs(
    hasher,
    project: ProjectManifest,
    sources: Mapping[str, SourceConfig],
    base_dir: Path,
) -> None:
    for source_id in sorted(sources):
        source = sources[source_id]
        for path, expands_glob in _source_input_patterns(source, project):
            _hash_source_pattern(hasher, path, expands_glob, base_dir)


def _stream_config_closure(
    root_stream_ids: Iterable[str],
    streams: StreamsConfig,
) -> tuple[dict[str, object], set[str]]:
    source_ids: set[str] = set()
    ingest_ids: set[str] = set()
    stream_ids: set[str] = set()

    def visit(stream_id: str) -> None:
        if stream_id in ingest_ids or stream_id in stream_ids:
            return
        ingest = streams.ingests.get(stream_id)
        if ingest is not None:
            ingest_ids.add(stream_id)
            source_ids.add(ingest.from_.source)
            return
        stream = streams.streams.get(stream_id)
        if stream is None:
            raise ValueError(f"Unknown stream '{stream_id}' in artifact input closure.")
        stream_ids.add(stream_id)
        for input_stream_id in stream.input_streams():
            visit(input_stream_id)

    for root_stream_id in root_stream_ids:
        visit(root_stream_id)

    config: dict[str, object] = {
        "sources": {
            source_id: streams.sources[source_id].model_dump(mode="json")
            for source_id in sorted(source_ids)
        },
        "ingests": {
            ingest_id: streams.ingests[ingest_id].model_dump(mode="json")
            for ingest_id in sorted(ingest_ids)
        },
        "streams": {
            stream_id: streams.streams[stream_id].model_dump(mode="json")
            for stream_id in sorted(stream_ids)
        },
    }
    return config, source_ids


def _artifact_inputs(
    task: ArtifactTask,
    dataset: FeatureDatasetConfig,
    streams: StreamsConfig,
) -> tuple[dict[str, object], set[str]]:
    if isinstance(task, TicksTask):
        stream_config, source_ids = _stream_config_closure((task.stream,), streams)
        return {"streams": stream_config}, source_ids

    if isinstance(task, ScalerTask):
        scaled = tuple(
            config for config in (*dataset.features, *dataset.targets) if config.scale
        )
        stream_config, source_ids = _stream_config_closure(
            (config.stream for config in scaled),
            streams,
        )
        return (
            {
                "dataset": {
                    "sample": dataset.sample.model_dump(mode="json"),
                    "split": (
                        None
                        if dataset.split is None
                        else dataset.split.model_dump(mode="json")
                    ),
                    "scaled_vectors": [
                        config.model_dump(mode="json", exclude={"sequence"})
                        for config in scaled
                    ],
                },
                "streams": stream_config,
            },
            source_ids,
        )

    if isinstance(task, VectorInputsTask):
        vectors = (*dataset.features, *dataset.targets)
        stream_config, source_ids = _stream_config_closure(
            (config.stream for config in vectors),
            streams,
        )
        return (
            {
                "dataset": {
                    "sample": dataset.sample.model_dump(mode="json"),
                    "features": [
                        config.model_dump(mode="json") for config in dataset.features
                    ],
                    "targets": [
                        config.model_dump(mode="json") for config in dataset.targets
                    ],
                },
                "streams": stream_config,
            },
            source_ids,
        )

    if isinstance(task, StatsTask) and task.stage == "postprocessed":
        return {"postprocess": dataset.postprocess.model_dump(mode="json")}, set()

    if isinstance(task, (MetadataTask, SchemaTask, StatsTask)):
        return {}, set()

    # Plugin artifacts receive the full Runtime and declare no input contract.
    # Their only safe cache boundary is the complete dataset and stream catalog.
    return (
        {
            "dataset": dataset.model_dump(mode="json"),
            "streams": streams.model_dump(mode="json"),
        },
        set(streams.sources),
    )


def _artifact_digest(
    project: ProjectManifest,
    task: ArtifactTask,
    dependencies: Mapping[str, str],
    inputs: Mapping[str, object],
    source_snapshots: Mapping[str, str],
) -> str:
    hasher = hashlib.sha256()
    hasher.update(f"[artifact-cache]{ARTIFACT_CACHE_VERSION}\0".encode("utf-8"))
    payload = {
        "artifact_revision": project.config.artifact_revision,
        "task": task.model_dump(mode="json"),
        "dependencies": dict(dependencies),
        "inputs": dict(inputs),
        "source_snapshots": dict(source_snapshots),
    }
    hasher.update(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    return hasher.hexdigest()


def calculate_artifact_hashes(
    project: ProjectManifest,
    dataset: FeatureDatasetConfig,
    streams: StreamsConfig,
    artifact_operations: tuple[ArtifactTask, ...],
) -> ArtifactHashes:
    base_dir = project.path.parent
    graph = build_artifact_graph(artifact_operations, dataset, streams)
    active_keys = graph.active_dependency_closure(
        graph.declared_artifact_keys(),
        dataset,
    )
    graph.validate_producers(active_keys)
    hashes: dict[str, str] = {}
    source_snapshot_cache: dict[str, str] = {}

    def source_snapshot(source_id: str) -> str:
        cached = source_snapshot_cache.get(source_id)
        if cached is not None:
            return cached
        hasher = hashlib.sha256()
        _hash_source_inputs(
            hasher,
            project,
            {source_id: streams.sources[source_id]},
            base_dir,
        )
        snapshot = hasher.hexdigest()
        source_snapshot_cache[source_id] = snapshot
        return snapshot

    for key in graph.topological_order(graph.declared_artifact_keys()):
        task = graph.tasks_by_id[key]
        dependency_hashes = {
            dependency: hashes[dependency]
            for dependency in graph.definition(key).dependencies
            if graph.definition(dependency).is_required_for(dataset)
        }
        inputs, source_ids = _artifact_inputs(task, dataset, streams)
        snapshots = {
            source_id: source_snapshot(source_id) for source_id in sorted(source_ids)
        }
        hashes[key] = _artifact_digest(
            project,
            task,
            dependency_hashes,
            inputs,
            snapshots,
        )
    return ArtifactHashes(hashes)


def calculate_definition_hash(
    project: ProjectManifest,
    documents: PipelineDocuments,
    dataset: FeatureDatasetConfig,
    streams: StreamsConfig,
    artifact_operations: tuple[ArtifactTask, ...],
    runtime_operations: tuple[OperationTask, ...],
) -> str:
    hasher = hashlib.sha256()
    base_dir = project.path.parent

    effective_definition = {
        "project": project.config.model_dump(mode="json"),
        "dataset": dataset.model_dump(mode="json"),
        "streams": streams.model_dump(mode="json"),
        "artifact_operations": [
            operation.model_dump(mode="json") for operation in artifact_operations
        ],
        "runtime_operations": [
            operation.model_dump(mode="json") for operation in runtime_operations
        ],
    }
    hasher.update(
        json.dumps(
            effective_definition,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    hasher.update(b"\0")

    _hash_document(hasher, project.document, base_dir)
    _hash_document(hasher, documents.dataset, base_dir)

    if project.operations_dir is not None:
        hasher.update(
            f"[dir]{_normalized_label(project.operations_dir, base_dir)}".encode(
                "utf-8"
            )
        )
    for document in documents.operations:
        _hash_document(hasher, document, base_dir)

    for directory in (*project.ingest_dirs, *project.source_dirs, *project.stream_dirs):
        hasher.update(f"[dir]{_normalized_label(directory, base_dir)}".encode("utf-8"))
    for document in (
        *documents.ingests,
        *documents.sources,
        *documents.streams,
    ):
        _hash_document(hasher, document, base_dir)

    _hash_env_refs(hasher, project, documents)
    return hasher.hexdigest()
