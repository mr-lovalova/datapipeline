from pathlib import Path

from datapipeline.artifacts.fingerprints import calculate_artifact_hashes
from datapipeline.artifacts.specs import dataset_requires_scaler
from datapipeline.config.tasks import ArtifactTask, OperationTask
from datapipeline.config.tasks.scaler import (
    ScalerTask,
    validate_scaler_task_for_dataset,
)
from datapipeline.services.dataset import (
    dataset_from_document,
    validate_dataset_streams,
)
from datapipeline.services.definitions import PipelineDefinition
from datapipeline.services.operations import (
    operation_documents,
    operations_from_documents,
)
from datapipeline.services.project import load_project
from datapipeline.services.streams.loader import load_streams
from datapipeline.utils.load import read_yaml_document


def load_pipeline(project_yaml: Path) -> PipelineDefinition:
    project = load_project(project_yaml)
    dataset_document = read_yaml_document(project.dataset_path)
    operation_config_documents = operation_documents(project)
    dataset = dataset_from_document(project, dataset_document)
    streams = load_streams(project)
    validate_dataset_streams(dataset, streams)
    operations = operations_from_documents(project, operation_config_documents)
    artifact_operations = tuple(
        operation for operation in operations if isinstance(operation, ArtifactTask)
    )
    if dataset_requires_scaler(dataset):
        scaler_task = next(
            task for task in artifact_operations if isinstance(task, ScalerTask)
        )
        validate_scaler_task_for_dataset(dataset, scaler_task)
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
