from datetime import datetime, timezone

import pytest

from datapipeline.domain.vector import Vector
from datapipeline.pipelines.full.split import HashLabeler, TimeLabeler


def test_hash_split_feature_key_errors_when_feature_is_missing():
    labeler = HashLabeler(
        ratios={"train": 0.5, "test": 0.5},
        key="feature:missing",
        seed=7,
    )

    with pytest.raises(KeyError, match="hash split feature key 'missing' not found"):
        labeler.label("group-a", Vector(values={"x": 1}))


def test_time_labeler_uses_boundaries():
    labeler = TimeLabeler(
        boundaries=["1970-01-02T00:00:00Z"],
        labels=["train", "test"],
    )
    vector = Vector(values={})

    assert labeler.label(datetime(1970, 1, 1, tzinfo=timezone.utc), vector) == "train"
    assert labeler.label(datetime(1970, 1, 2, tzinfo=timezone.utc), vector) == "test"
