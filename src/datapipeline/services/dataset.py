from datapipeline.config.catalog import StreamsConfig
from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.services.config_refs import (
    interpolate_config_vars,
    resolve_config_refs,
)
from datapipeline.services.definitions import ProjectManifest
from datapipeline.services.streams.validation import (
    stream_feature_id_by,
    stream_partition_by,
)
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
        feature_id_by = stream_feature_id_by(
            streams.ingests,
            streams.streams,
            config.stream,
        )
        if partition_by and feature_id_by is None:
            raise ValueError(
                f"Stream '{config.stream}' has partition_by and is used as a "
                "dataset feature or target. Set feature_id_by: [] for scalar "
                "keyed features, or set feature_id_by to the fields that should "
                "suffix feature ids."
            )
        identity_fields = set(dataset.sample.keys) | set(feature_id_by or ())
        missing_partition_fields = [
            field for field in partition_by if field not in identity_fields
        ]
        if missing_partition_fields:
            raise ValueError(
                f"Stream '{config.stream}' does not preserve partition identity "
                f"for {missing_partition_fields!r}. Add those fields to sample.keys "
                "or feature_id_by."
            )
        if config.sequence is not None and identity_fields != set(partition_by):
            raise ValueError(
                f"Sequenced dataset vector '{config.id}' requires sample.keys and "
                f"feature_id_by to match stream '{config.stream}' partition_by "
                "exactly."
            )
