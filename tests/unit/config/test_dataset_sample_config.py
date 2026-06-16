import pytest

from datapipeline.config.dataset.dataset import FeatureDatasetConfig


def test_group_by_normalizes_to_time_only_sample_config() -> None:
    dataset = FeatureDatasetConfig.model_validate(
        {"group_by": "1d", "features": [], "targets": []}
    )

    assert dataset.group_by == "1d"
    assert dataset.sample is not None
    assert dataset.sample.cadence == "1d"
    assert dataset.sample_keys == []


def test_sample_config_sets_group_by_compatibility_value() -> None:
    dataset = FeatureDatasetConfig.model_validate(
        {
            "sample": {"cadence": "1d", "keys": ["security_id"]},
            "features": [],
            "targets": [],
        }
    )

    assert dataset.group_by == "1d"
    assert dataset.sample_keys == ["security_id"]


def test_group_by_must_match_sample_cadence() -> None:
    with pytest.raises(ValueError, match="group_by must match sample.cadence"):
        FeatureDatasetConfig.model_validate(
            {
                "group_by": "1h",
                "sample": {"cadence": "1d"},
                "features": [],
                "targets": [],
            }
        )


def test_feature_dataset_requires_cadence() -> None:
    with pytest.raises(ValueError, match="sample.cadence or group_by"):
        FeatureDatasetConfig.model_validate({"features": [], "targets": []})
