import pytest
from pydantic import ValidationError

from datapipeline.config.dataset.dataset import FeatureDatasetConfig


def test_feature_dataset_requires_sample_config() -> None:
    with pytest.raises(ValidationError, match="sample"):
        FeatureDatasetConfig.model_validate({"features": [], "targets": []})


def test_feature_dataset_loads_sample_cadence_and_keys() -> None:
    dataset = FeatureDatasetConfig.model_validate(
        {
            "sample": {"cadence": "1d", "keys": ["security_id"]},
            "features": [],
            "targets": [],
        }
    )

    assert dataset.sample.cadence == "1d"
    assert dataset.sample.keys == ["security_id"]


def test_feature_dataset_owns_split_and_postprocess_policy() -> None:
    dataset = FeatureDatasetConfig.model_validate(
        {
            "sample": {"cadence": "1d"},
            "split": {
                "mode": "hash",
                "key": "group",
                "ratios": {"train": 0.8, "test": 0.2},
            },
            "postprocess": {
                "samples": {"features": {"threshold": 0.9}},
            },
        }
    )

    assert dataset.split is not None
    assert dataset.postprocess.samples.features is not None
    assert dataset.postprocess.samples.features.threshold == 0.9


def test_feature_dataset_rejects_legacy_group_by() -> None:
    with pytest.raises(ValidationError, match="group_by"):
        FeatureDatasetConfig.model_validate(
            {
                "group_by": "1d",
                "sample": {"cadence": "1d"},
            }
        )


def test_feature_dataset_rejects_zero_cadence() -> None:
    with pytest.raises(ValidationError):
        FeatureDatasetConfig.model_validate({"sample": {"cadence": "0min"}})


@pytest.mark.parametrize("field", ["features", "targets"])
def test_feature_dataset_rejects_null_record_lists(field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        FeatureDatasetConfig.model_validate({"sample": {"cadence": "1d"}, field: None})


@pytest.mark.parametrize("keys", [[""], ["security_id", "security_id"]])
def test_feature_dataset_rejects_invalid_sample_keys(keys: list[str]) -> None:
    with pytest.raises(ValidationError, match="at least 1 character|sample keys"):
        FeatureDatasetConfig.model_validate({"sample": {"cadence": "1d", "keys": keys}})


@pytest.mark.parametrize("duplicate_section", ["features", "targets"])
def test_feature_dataset_rejects_duplicate_vector_ids(
    duplicate_section: str,
) -> None:
    vector = {"id": "price", "stream": "prices", "field": "close"}
    payload = {
        "sample": {"cadence": "1d"},
        "features": [vector],
        "targets": [],
    }
    payload[duplicate_section] = [vector, vector]

    with pytest.raises(ValidationError, match="must be unique"):
        FeatureDatasetConfig.model_validate(payload)


def test_feature_dataset_rejects_vector_id_shared_by_feature_and_target() -> None:
    vector = {"id": "price", "stream": "prices", "field": "close"}

    with pytest.raises(ValidationError, match="must be unique"):
        FeatureDatasetConfig.model_validate(
            {
                "sample": {"cadence": "1d"},
                "features": [vector],
                "targets": [vector],
            }
        )
