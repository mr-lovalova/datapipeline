from typing import TYPE_CHECKING

from datapipeline.config.dataset.dataset import FeatureDatasetConfig

if TYPE_CHECKING:
    from datapipeline.runtime import Runtime


def validate_dataset_feature_identity(
    runtime: "Runtime",
    dataset: FeatureDatasetConfig,
) -> None:
    for cfg in [*dataset.features, *dataset.targets]:
        partition_by = runtime.registries.partition_by.get(cfg.record_stream)
        feature_id_by = runtime.registries.feature_id_by.get(cfg.record_stream)
        if partition_by is not None and feature_id_by is None:
            raise ValueError(
                f"Stream '{cfg.record_stream}' has partition_by and is used as a "
                "dataset feature or target. Set feature_id_by: [] for scalar "
                "keyed features, or set feature_id_by to the fields that should "
                "suffix feature ids."
            )
