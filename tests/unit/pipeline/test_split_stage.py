from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from datapipeline.config.split import TimeSplitConfig
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.pipelines.full.split import (
    HashLabeler,
    VectorSplitApplicator,
    apply_split_stage,
)


def _vector_stream():
    return iter(
        [
            Sample(
                key=datetime(1970, 1, 1, tzinfo=timezone.utc),
                features=Vector(values={"x": 1}),
            ),
            Sample(
                key=datetime(1970, 1, 3, tzinfo=timezone.utc),
                features=Vector(values={"x": 2}),
            ),
        ]
    )


def _time_split_config(**kwargs) -> TimeSplitConfig:
    return TimeSplitConfig(
        boundaries=["1970-01-02T00:00:00Z"],
        labels=["alpha", "beta"],
        **kwargs,
    )


def test_split_stage_filters_when_runtime_keep_set():
    cfg = _time_split_config()
    runtime = SimpleNamespace(split=cfg, split_keep="beta")

    filtered = list(apply_split_stage(runtime, _vector_stream()))

    assert len(filtered) == 1
    assert filtered[0].features.values["x"] == 2


def test_split_stage_passes_through_when_keep_missing():
    cfg = _time_split_config()
    runtime = SimpleNamespace(split=cfg, split_keep=None)

    filtered = list(apply_split_stage(runtime, _vector_stream()))

    assert len(filtered) == 2


def test_split_stage_passes_through_when_keep_is_placeholder():
    cfg = _time_split_config()
    runtime = SimpleNamespace(split=cfg, split_keep="${split}")

    filtered = list(apply_split_stage(runtime, _vector_stream()))

    assert len(filtered) == 2


def test_hash_split_feature_key_errors_when_feature_is_missing():
    vector = Vector(values={"x": 1})
    labeler = HashLabeler(
        ratios={"train": 0.5, "test": 0.5},
        key="feature:missing",
        seed=7,
    )

    with pytest.raises(KeyError, match="hash split feature key 'missing' not found"):
        labeler.label("group-a", vector)


def test_split_applicator_can_tag_samples():
    labeler = HashLabeler(ratios={"train": 1.0}, seed=7)
    applicator = VectorSplitApplicator(
        labeler=labeler,
        output="tag",
        field="split",
    )

    tagged = list(applicator.apply(_vector_stream()))

    assert [sample.features.values["split"] for sample in tagged] == [
        "train",
        "train",
    ]
    assert all("__split__" not in sample.features.values for sample in tagged)
