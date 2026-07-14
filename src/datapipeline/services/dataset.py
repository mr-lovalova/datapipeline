from datapipeline.config.catalog import StreamsConfig
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
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
) -> FeatureDatasetConfig:
    data = resolve_config_refs(
        document.data,
        project_yaml=project.path,
        env=project.environment,
    )
    data = interpolate_config_vars(data, project.variables)

    return FeatureDatasetConfig.model_validate(data)


def validate_dataset_streams(
    dataset: FeatureDatasetConfig,
    streams: StreamsConfig,
) -> None:
    for config in (*dataset.features, *dataset.targets):
        if (
            config.stream not in streams.ingests
            and config.stream not in streams.streams
        ):
            raise ValueError(
                f"Dataset vector '{config.id}' references unknown stream "
                f"'{config.stream}'."
            )
        partition_by = stream_partition_by(
            streams.ingests,
            streams.streams,
            config.stream,
        )
        missing_sample_keys = [
            field for field in dataset.sample.keys if field not in partition_by
        ]
        if missing_sample_keys:
            raise ValueError(
                f"Dataset vector '{config.id}' uses sample.keys "
                f"{missing_sample_keys!r} that are not part of stream "
                f"'{config.stream}' partition_by {list(partition_by)!r}."
            )
