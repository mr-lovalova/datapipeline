from datetime import datetime, timezone

import pytest

from datapipeline.config.dataset.split import (
    DatasetFold,
    HashSplitConfig,
    TimeInterval,
    TimeSplitConfig,
)
from datapipeline.pipelines.dataset.split import HashLabeler, TimeLabeler


def _fold() -> DatasetFold:
    return DatasetFold(
        id="default",
        train=["train"],
        validation=["val"],
        test=["test"],
    )


def test_hash_split_ratio_mapping_order_does_not_change_labels() -> None:
    first = HashLabeler(
        HashSplitConfig(
            ratios={"train": 0.7, "val": 0.2, "test": 0.1},
            folds=[_fold()],
            seed=7,
        )
    )
    second = HashLabeler(
        HashSplitConfig(
            ratios={"test": 0.1, "train": 0.7, "val": 0.2},
            folds=[_fold()],
            seed=7,
        )
    )
    assert [first.label(index) for index in range(1_000)] == [
        second.label(index) for index in range(1_000)
    ]


def test_hash_split_uses_the_whole_group_key() -> None:
    labeler = HashLabeler(
        HashSplitConfig(
            ratios={"train": 0.5, "test": 0.5},
            folds=[
                DatasetFold(id="default", train=["train"], test=["test"]),
            ],
            seed=7,
        )
    )

    first = [
        labeler.label(("A", index))
        for index in range(100)
    ]
    second = [
        labeler.label(("B", index))
        for index in range(100)
    ]

    assert first != second


def test_time_labeler_uses_intervals():
    labeler = TimeLabeler(
        TimeSplitConfig(
            intervals=[
                TimeInterval(id="train", until="1970-01-02T00:00:00Z"),
                TimeInterval(id="test"),
            ],
            folds=[DatasetFold(id="default", train=["train"], test=["test"])],
        )
    )
    assert labeler.label(datetime(1970, 1, 1, tzinfo=timezone.utc)) == "train"
    assert labeler.label(datetime(1970, 1, 2, tzinfo=timezone.utc)) == "test"


def test_time_labeler_accepts_iso_string_keys():
    labeler = TimeLabeler(
        TimeSplitConfig(
            intervals=[
                TimeInterval(id="train", until="1970-01-02T00:00:00Z"),
                TimeInterval(id="test"),
            ],
            folds=[DatasetFold(id="default", train=["train"], test=["test"])],
        )
    )

    assert labeler.label("1970-01-02T00:00:00Z") == "test"


def test_time_labeler_rejects_ambiguous_keys():
    labeler = TimeLabeler(
        TimeSplitConfig(
            intervals=[
                TimeInterval(id="train", until="1970-01-02T00:00:00Z"),
                TimeInterval(id="test"),
            ],
            folds=[DatasetFold(id="default", train=["train"], test=["test"])],
        )
    )

    with pytest.raises(TypeError, match="datetimes or ISO-8601 strings"):
        labeler.label(1)
