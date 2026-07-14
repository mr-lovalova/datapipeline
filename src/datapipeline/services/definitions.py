from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from datapipeline.config.catalog import StreamsConfig
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.config.project import ProjectConfig
from datapipeline.config.tasks import ArtifactTask, OperationTask
from datapipeline.utils.load import YamlDocument


@dataclass(frozen=True, slots=True)
class ProjectManifest:
    path: Path
    document: YamlDocument
    config: ProjectConfig
    variables: Mapping[str, Any]
    environment: Mapping[str, str]
    ingest_dirs: tuple[Path, ...]
    stream_dirs: tuple[Path, ...]
    source_dirs: tuple[Path, ...]
    dataset_path: Path
    artifacts_root: Path
    operations_dir: Path | None
    profiles_dir: Path


@dataclass(frozen=True, slots=True)
class PipelineDocuments:
    dataset: YamlDocument
    sources: tuple[YamlDocument, ...]
    ingests: tuple[YamlDocument, ...]
    streams: tuple[YamlDocument, ...]
    operations: tuple[YamlDocument, ...]


@dataclass(frozen=True, slots=True)
class ArtifactHashes:
    values: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))

    def for_artifact(self, key: str) -> str:
        return self.values[key]


@dataclass(frozen=True, slots=True)
class PipelineDefinition:
    project: ProjectManifest
    dataset: FeatureDatasetConfig
    streams: StreamsConfig
    artifact_operations: tuple[ArtifactTask, ...]
    runtime_operations: tuple[OperationTask, ...]
    documents: PipelineDocuments
    definition_hash: str
    artifact_hashes: ArtifactHashes
