from datapipeline.config.dataset.dataset import FeatureDatasetConfig
from datapipeline.runtime import Runtime, require_runtime_stream


def validate_dataset_feature_identity(
    runtime: Runtime,
    dataset: FeatureDatasetConfig,
) -> None:
    for cfg in [*dataset.features, *dataset.targets]:
        stream = require_runtime_stream(runtime, cfg.record_stream)
        if stream.partition_by is None:
            continue
        if stream.feature_id_by is None:
            raise ValueError(
                f"Stream '{cfg.record_stream}' has partition_by and is used as a "
                "dataset feature or target. Set feature_id_by: [] for scalar "
                "keyed features, or set feature_id_by to the fields that should "
                "suffix feature ids."
            )
        if stream.feature_id_by != []:
            continue

        partition_fields = (
            [stream.partition_by]
            if isinstance(stream.partition_by, str)
            else stream.partition_by
        )
        missing_sample_keys = [
            field for field in partition_fields if field not in dataset.sample.keys
        ]
        if missing_sample_keys:
            raise ValueError(
                f"Stream '{cfg.record_stream}' uses feature_id_by: [], so its "
                "partition fields must identify dataset samples. Add "
                f"{missing_sample_keys!r} to sample.keys or suffix feature ids "
                "with feature_id_by."
            )
