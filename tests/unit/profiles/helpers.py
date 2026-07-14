from collections.abc import Sequence
from pathlib import Path

from datapipeline.config.catalog import StreamsConfig
from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.project import ProjectConfig
from datapipeline.config.tasks import ArtifactTask, OperationTask
from datapipeline.services.definitions import (
    ArtifactHashes,
    PipelineDefinition,
    ProjectManifest,
)


def pipeline_definition(
    project_path: Path,
    *,
    dataset: FeatureDatasetConfig | None = None,
    streams: StreamsConfig | None = None,
    artifact_operations: Sequence[ArtifactTask] = (),
    runtime_operations: Sequence[OperationTask] = (),
    artifact_hash: str = "artifact-hash",
) -> PipelineDefinition:
    project_path = project_path.resolve()
    root = project_path.parent
    project = ProjectManifest(
        path=project_path,
        config=ProjectConfig.model_validate(
            {
                "version": 1,
                "artifact_revision": 1,
                "paths": {
                    "ingests": "ingests",
                    "streams": "streams",
                    "sources": "sources",
                    "dataset": "dataset.yaml",
                    "artifacts": "artifacts",
                    "operations": "operations",
                    "profiles": "profiles",
                },
            }
        ),
        variables={},
        environment={},
        ingest_dirs=(root / "ingests",),
        stream_dirs=(root / "streams",),
        source_dirs=(root / "sources",),
        dataset_path=root / "dataset.yaml",
        artifacts_root=root / "artifacts",
        operations_dir=root / "operations",
        profiles_dir=root / "profiles",
    )
    return PipelineDefinition(
        project=project,
        dataset=(
            dataset
            if dataset is not None
            else FeatureDatasetConfig(sample=SampleConfig(cadence="1h"))
        ),
        streams=streams if streams is not None else StreamsConfig(),
        artifact_operations=tuple(artifact_operations),
        runtime_operations=tuple(runtime_operations),
        artifact_hashes=ArtifactHashes(
            {operation.id: artifact_hash for operation in artifact_operations}
        ),
    )
