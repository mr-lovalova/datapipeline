from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from datapipeline.config.split import HashSplitConfig, TimeSplitConfig
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.pipelines.full.split import apply_split_stage


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


def test_split_stage_raises_for_incomplete_time_split_config():
    runtime = SimpleNamespace(
        split=TimeSplitConfig(boundaries=None, labels=None),
        split_keep="beta",
    )

    with pytest.raises(ValueError, match="time split requires"):
        list(apply_split_stage(runtime, _vector_stream()))


def test_split_stage_raises_for_incomplete_hash_split_config():
    runtime = SimpleNamespace(split=HashSplitConfig(ratios=None), split_keep="train")

    with pytest.raises(ValueError, match="hash split requires"):
        list(apply_split_stage(runtime, _vector_stream()))
