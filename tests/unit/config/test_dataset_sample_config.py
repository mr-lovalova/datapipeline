import pytest
from pydantic import ValidationError

from datapipeline.config.dataset.dataset import DatasetConfig


def test_dataset_requires_sample_config() -> None:
    with pytest.raises(ValidationError, match="sample"):
        DatasetConfig.model_validate({"features": [], "targets": []})


def test_dataset_loads_sample_cadence_and_keys() -> None:
    dataset = DatasetConfig.model_validate(
        {
            "sample": {"cadence": "1d", "keys": ["security_id"]},
            "features": [],
            "targets": [],
        }
    )

    assert dataset.sample.cadence == "1d"
    assert dataset.sample.keys == ["security_id"]


def test_dataset_rejects_targets_without_features() -> None:
    with pytest.raises(ValidationError, match="must define at least one feature"):
        DatasetConfig.model_validate(
            {
                "sample": {"cadence": "1d"},
                "targets": [{"id": "return", "stream": "returns", "field": "value"}],
            }
        )


def test_dataset_owns_split_and_postprocess_policy() -> None:
    dataset = DatasetConfig.model_validate(
        {
            "sample": {"cadence": "1d"},
            "split": {
                "mode": "hash",
                "ratios": {"train": 0.8, "test": 0.2},
                "folds": [
                    {
                        "id": "holdout",
                        "train": ["train"],
                        "test": ["test"],
                    }
                ],
            },
            "postprocess": {
                "samples": {"features": {"threshold": 0.9}},
            },
        }
    )

    assert dataset.split is not None
    assert dataset.postprocess.samples.features is not None
    assert dataset.postprocess.samples.features.threshold == 0.9


def test_dataset_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DatasetConfig.model_validate(
            {
                "sample": {"cadence": "1d"},
                "unexpected": True,
            }
        )


def test_dataset_rejects_zero_cadence() -> None:
    with pytest.raises(ValidationError):
        DatasetConfig.model_validate({"sample": {"cadence": "0min"}})


@pytest.mark.parametrize("field", ["features", "targets"])
def test_dataset_rejects_null_series_lists(field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        DatasetConfig.model_validate({"sample": {"cadence": "1d"}, field: None})


@pytest.mark.parametrize("keys", [[""], ["security_id", "security_id"]])
def test_dataset_rejects_invalid_sample_keys(keys: list[str]) -> None:
    with pytest.raises(ValidationError, match="at least 1 character|sample keys"):
        DatasetConfig.model_validate({"sample": {"cadence": "1d", "keys": keys}})


@pytest.mark.parametrize("duplicate_section", ["features", "targets"])
def test_dataset_rejects_duplicate_series_ids(
    duplicate_section: str,
) -> None:
    series = {"id": "price", "stream": "prices", "field": "close"}
    payload = {
        "sample": {"cadence": "1d"},
        "features": [series],
        "targets": [],
    }
    payload[duplicate_section] = [series, series]

    with pytest.raises(ValidationError, match="must be unique"):
        DatasetConfig.model_validate(payload)


def test_dataset_rejects_series_id_shared_by_feature_and_target() -> None:
    series = {"id": "price", "stream": "prices", "field": "close"}

    with pytest.raises(ValidationError, match="must be unique"):
        DatasetConfig.model_validate(
            {
                "sample": {"cadence": "1d"},
                "features": [series],
                "targets": [series],
            }
        )


def test_dataset_series_preserves_feature_then_target_order() -> None:
    dataset = DatasetConfig.model_validate(
        {
            "sample": {"cadence": "1d"},
            "features": [{"id": "price", "stream": "prices", "field": "close"}],
            "targets": [{"id": "return", "stream": "returns", "field": "value"}],
        }
    )

    assert [series.id for series in dataset.series] == ["price", "return"]
    assert "series" not in dataset.model_dump()
