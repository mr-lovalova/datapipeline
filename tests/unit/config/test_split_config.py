import pytest
from pydantic import TypeAdapter, ValidationError

from datapipeline.config.split import HashSplitConfig, SplitConfig, TimeSplitConfig


def test_hash_split_accepts_group_key() -> None:
    cfg = HashSplitConfig(ratios={"train": 1.0}, key="group")

    assert cfg.key == "group"


def test_hash_split_accepts_feature_key() -> None:
    cfg = HashSplitConfig(ratios={"train": 1.0}, key="feature:ticker")

    assert cfg.key == "feature:ticker"


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
