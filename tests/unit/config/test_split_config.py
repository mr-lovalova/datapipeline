import pytest
from pydantic import TypeAdapter, ValidationError

from datapipeline.config.dataset.split import (
    HashSplitConfig,
    SplitConfig,
    TimeSplitConfig,
    split_output_labels,
)


def test_hash_split_accepts_group_key() -> None:
    cfg = HashSplitConfig(ratios={"train": 1.0}, key="group")

    assert cfg.key == "group"


def test_hash_split_accepts_feature_key() -> None:
    cfg = HashSplitConfig(ratios={"train": 1.0}, key="feature:ticker")

    assert cfg.key == "feature:ticker"


def test_hash_split_canonicalizes_ratio_order() -> None:
    first = HashSplitConfig(ratios={"train": 0.8, "test": 0.2})
    second = HashSplitConfig(ratios={"test": 0.2, "train": 0.8})

    assert list(first.ratios.items()) == [("test", 0.2), ("train", 0.8)]
    assert first.ratios == second.ratios


def test_split_output_labels_default_to_all_defined_labels() -> None:
    hash_split = HashSplitConfig(ratios={"train": 0.8, "test": 0.2})
    time_split = TimeSplitConfig(
        boundaries=["2024-01-01T00:00:00Z"],
        labels=["train", "test"],
    )

    assert split_output_labels(hash_split) == ("test", "train")
    assert split_output_labels(time_split) == ("train", "test")


def test_split_output_labels_preserve_configured_subset_order() -> None:
    config = HashSplitConfig(
        ratios={"train": 0.8, "val": 0.1, "test": 0.1},
        output_labels=["train", "val"],
    )

    assert split_output_labels(config) == ("train", "val")


def test_time_split_output_labels_can_omit_internal_intervals() -> None:
    config = TimeSplitConfig(
        boundaries=["2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z"],
        labels=["train", "purge", "val"],
        output_labels=["train", "val"],
    )

    assert split_output_labels(config) == ("train", "val")


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"ratios": {"train": 1.0}, "key": "unknown"}, "hash split key must be"),
        ({"ratios": {"train": 1.0}, "key": "feature:"}, "include a feature id"),
        ({"mode": "hash"}, "ratios"),
        ({"ratios": {}}, "ratios must not be empty"),
        ({"ratios": {"train": 0.0, "test": 1.0}}, "greater than 0"),
        ({"ratios": {"train": 0.5, "test": 0.4}}, "must sum to 1.0"),
        ({"ratios": {"": 1.0}}, "labels must not be empty"),
        ({"ratios": {" train ": 1.0}}, "outer whitespace"),
        ({"ratios": {"train": 1.0}, "seed": "42"}, "valid integer"),
        (
            {"ratios": {"train": 1.0}, "output_labels": ["test"]},
            "not defined by ratios",
        ),
        (
            {"ratios": {"train": 1.0}, "output_labels": ["train", "train"]},
            "must be unique",
        ),
        ({"ratios": {"train": 1.0}, "unknown": True}, "Extra inputs"),
    ],
)
def test_hash_split_rejects_invalid_config(payload, message) -> None:
    with pytest.raises(ValidationError, match=message):
        HashSplitConfig.model_validate(payload)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"mode": "time", "labels": ["train"]}, "boundaries"),
        ({"mode": "time", "boundaries": []}, "labels"),
        (
            {
                "boundaries": ["2024-01-01T00:00:00Z"],
                "labels": ["train"],
            },
            "labels length",
        ),
        (
            {
                "mode": "time",
                "boundaries": ["not-a-datetime"],
                "labels": ["train", "test"],
            },
            "Invalid ISO-8601 datetime",
        ),
        (
            {
                "mode": "time",
                "boundaries": [
                    "2024-02-01T00:00:00Z",
                    "2024-01-01T00:00:00Z",
                ],
                "labels": ["train", "val", "test"],
            },
            "strictly increasing",
        ),
        (
            {
                "mode": "time",
                "boundaries": ["2024-01-01T00:00:00Z"],
                "labels": ["train", "train"],
            },
            "labels must be unique",
        ),
        (
            {
                "mode": "time",
                "boundaries": ["2024-01-01T00:00:00Z"],
                "labels": ["train", " "],
            },
            "labels must not be empty",
        ),
        (
            {
                "mode": "time",
                "boundaries": ["2024-01-01T00:00:00Z"],
                "labels": ["train", "test"],
                "output_labels": ["val"],
            },
            "output labels are not defined by labels",
        ),
        (
            {
                "mode": "time",
                "boundaries": ["2024-01-01T00:00:00Z"],
                "labels": ["train", "test"],
                "output_labels": ["train", "train"],
            },
            "output labels must be unique",
        ),
    ],
)
def test_time_split_rejects_invalid_config(payload, message) -> None:
    with pytest.raises(ValidationError, match=message):
        TimeSplitConfig.model_validate(payload)


def test_split_union_requires_an_explicit_mode() -> None:
    with pytest.raises(ValidationError, match="mode"):
        TypeAdapter(SplitConfig).validate_python({"ratios": {"train": 1.0}})


def test_split_config_is_frozen() -> None:
    config = HashSplitConfig(ratios={"train": 1.0})

    with pytest.raises(ValidationError, match="frozen"):
        config.seed = 7
