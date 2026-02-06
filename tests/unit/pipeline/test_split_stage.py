from datetime import datetime, timezone
from types import SimpleNamespace

from datapipeline.config.split import TimeSplitConfig
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.pipeline.split import apply_split_stage


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
