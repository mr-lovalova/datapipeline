from pathlib import Path

from datapipeline.services.pipeline_fingerprints import calculate_artifact_hashes
from datapipeline.services.operations import (
    operation_documents,
    operations_from_documents,
)
from datapipeline.config.tasks import ArtifactTask, OperationTask
from datapipeline.services.streams.loader import (
    stream_documents,
    streams_from_documents,
)
from datapipeline.services.dataset import (
    dataset_from_document,
    validate_dataset_streams,
)
from datapipeline.services.definitions import PipelineDefinition
from datapipeline.services.project import load_project
from datapipeline.utils.load import read_yaml_document


def load_pipeline(project_yaml: Path) -> PipelineDefinition:
    project = load_project(project_yaml)
    dataset_document = read_yaml_document(project.dataset_path)
    source_documents, ingest_documents, stream_config_documents = stream_documents(
        project
    )
    operation_config_documents = operation_documents(project)
    dataset = dataset_from_document(project, dataset_document)
    streams = streams_from_documents(
        project,
        source_documents,
        ingest_documents,
        stream_config_documents,
    )
    validate_dataset_streams(dataset, streams)
    operations = operations_from_documents(project, operation_config_documents)
    artifact_operations = tuple(
        operation for operation in operations if isinstance(operation, ArtifactTask)
    )
    runtime_operations = tuple(
        operation for operation in operations if isinstance(operation, OperationTask)
    )
    artifact_hashes = calculate_artifact_hashes(
        project,
        dataset,
        streams,
        artifact_operations,
    )
    return PipelineDefinition(
        project=project,
        dataset=dataset,
        streams=streams,
        artifact_operations=artifact_operations,
        runtime_operations=runtime_operations,
        artifact_hashes=artifact_hashes,
    )
