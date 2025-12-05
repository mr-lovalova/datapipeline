from __future__ import annotations

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector import VectorFillTransform, VectorReplaceTransform
from tests.unit.transforms.helpers import StubVectorContext, make_vector


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
