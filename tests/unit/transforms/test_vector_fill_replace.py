from datetime import datetime, timezone

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector import (
    VectorFillTransform,
    VectorForwardFillTransform,
    VectorReplaceTransform,
)
from tests.unit.transforms.helpers import StubVectorContext, make_vector


def _dt(month: int, day: int) -> datetime:
    return datetime(2024, month, day, tzinfo=timezone.utc)


def test_vector_fill_history_uses_running_statistics():
    stream = iter(
        [
            make_vector(0, {"temp__A": 10.0}),
            make_vector(1, {"temp__A": 12.0}),
            make_vector(2, {}),
        ]
    )

    transform = VectorFillTransform(statistic="mean", window=2, min_samples=2)
    transform.bind_context(StubVectorContext(["temp__A"]))

    out = list(transform.apply(stream))
    assert out[2].features.values["temp__A"] == 11.0


def test_vector_fill_history_is_isolated_by_sample_entity():
    stream = iter(
        [
            Sample(key=(0, "AAPL"), features=Vector(values={"gross_margin": 1.0})),
            Sample(key=(0, "MSFT"), features=Vector(values={"gross_margin": 10.0})),
            Sample(key=(1, "AAPL"), features=Vector(values={"gross_margin": None})),
        ]
    )

    transform = VectorFillTransform(statistic="median", window=1, min_samples=1)
    transform.bind_context(StubVectorContext(["gross_margin"]))

    out = list(transform.apply(stream))

    assert out[2].features.values["gross_margin"] == 1.0


def test_vector_forward_fill_carries_latest_value_by_sample_entity():
    stream = iter(
        [
            Sample(
                key=(_dt(1, 31), "AAPL"),
                features=Vector(values={"gross_margin": 0.44}),
            ),
            Sample(
                key=(_dt(1, 31), "MSFT"),
                features=Vector(values={"gross_margin": 0.61}),
            ),
            Sample(
                key=(_dt(2, 29), "AAPL"),
                features=Vector(values={"gross_margin": None}),
            ),
            Sample(
                key=(_dt(2, 29), "MSFT"),
                features=Vector(values={"gross_margin": None}),
            ),
        ]
    )

    transform = VectorForwardFillTransform(only=["gross_margin"])
    transform.bind_context(StubVectorContext(["gross_margin"]))

    out = list(transform.apply(stream))

    assert out[2].features.values["gross_margin"] == 0.44
    assert out[3].features.values["gross_margin"] == 0.61


def test_vector_forward_fill_respects_max_age():
    stream = iter(
        [
            Sample(
                key=(_dt(1, 1), "AAPL"),
                features=Vector(values={"gross_margin": 0.44}),
            ),
            Sample(
                key=(_dt(3, 1), "AAPL"),
                features=Vector(values={"gross_margin": None}),
            ),
        ]
    )

    transform = VectorForwardFillTransform(only=["gross_margin"], max_age="30d")
    transform.bind_context(StubVectorContext(["gross_margin"]))

    out = list(transform.apply(stream))

    assert out[1].features.values["gross_margin"] is None


def test_vector_forward_fill_rejects_out_of_order_max_age():
    stream = iter(
        [
            Sample(
                key=(_dt(3, 1), "AAPL"),
                features=Vector(values={"gross_margin": 0.44}),
            ),
            Sample(
                key=(_dt(1, 1), "AAPL"),
                features=Vector(values={"gross_margin": None}),
            ),
        ]
    )

    transform = VectorForwardFillTransform(only=["gross_margin"], max_age="540d")
    transform.bind_context(StubVectorContext(["gross_margin"]))

    out = list(transform.apply(stream))

    assert out[1].features.values["gross_margin"] is None


def test_vector_fill_constant_injects_value():
    stream = iter([make_vector(0, {"time": 1.0})])
    transform = VectorReplaceTransform(value=0.0)
    transform.bind_context(StubVectorContext(["time", "wind"]))
    out = list(transform.apply(stream))
    assert out[0].features.values["wind"] == 0.0


def test_vector_fill_constant_targets_payload():
    sample = Sample(
        key=(0,),
        features=Vector(values={"f": 1.0}),
        targets=Vector(values={"t": None}),
    )
    transform = VectorReplaceTransform(value=5.0, payload="targets")
    transform.bind_context(StubVectorContext({"targets": ["t"]}))

    out = list(transform.apply(iter([sample])))

    assert out[0].features.values == {"f": 1.0}
    assert out[0].targets is not None
    assert out[0].targets.values["t"] == 5.0
