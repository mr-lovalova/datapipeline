from datapipeline.config.dataset.dataset import DatasetConfig
from datapipeline.config.streams import StreamsConfig
from datapipeline.services.config_refs import (
    interpolate_config_vars,
    resolve_config_refs,
)
from datapipeline.services.definitions import ProjectManifest
from datapipeline.services.streams.validation import stream_partition_by
from datapipeline.utils.load import YamlDocument


def dataset_from_document(
    project: ProjectManifest,
    document: YamlDocument,
) -> DatasetConfig:
    data = resolve_config_refs(
        document.data,
        project_yaml=project.path,
        env=project.environment,
    )
    data = interpolate_config_vars(data, project.variables)

    return DatasetConfig.model_validate(data)


def validate_dataset_streams(
    dataset: DatasetConfig,
    streams: StreamsConfig,
) -> None:
    for config in dataset.variables:
        if config.stream not in streams.streams:
            raise ValueError(
                f"Dataset variable '{config.id}' references unknown stream "
                f"'{config.stream}'."
            )
        partition_by = stream_partition_by(streams.streams, config.stream)
        missing_sample_keys = [
            field for field in dataset.sample.keys if field not in partition_by
        ]
        if missing_sample_keys:
            raise ValueError(
                f"Dataset variable '{config.id}' uses sample.keys "
                f"{missing_sample_keys!r} that are not part of stream "
                f"'{config.stream}' partition_by {list(partition_by)!r}."
            )
