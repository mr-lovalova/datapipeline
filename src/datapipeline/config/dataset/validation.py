from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.runtime import Runtime, require_runtime_stream


def validate_dataset_feature_identity(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
) -> None:
    for cfg in [*dataset.features, *dataset.targets]:
        stream = require_runtime_stream(runtime, cfg.record_stream)
        if not stream.partition_by:
            continue
        if stream.feature_id_by is None:
            raise ValueError(
                f"Stream '{cfg.record_stream}' has partition_by and is used as a "
                "dataset feature or target. Set feature_id_by: [] for scalar "
                "keyed features, or set feature_id_by to the fields that should "
                "suffix feature ids."
            )
        identity_fields = set(dataset.sample.keys) | set(stream.feature_id_by)
        missing_partition_fields = [
            field for field in stream.partition_by if field not in identity_fields
        ]
        if missing_partition_fields:
            raise ValueError(
                f"Stream '{cfg.record_stream}' does not preserve partition identity "
                f"for {missing_partition_fields!r}. Add those fields to sample.keys "
                "or feature_id_by."
            )
