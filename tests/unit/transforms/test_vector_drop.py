from __future__ import annotations

import pytest

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector import VectorDropTransform
from tests.unit.transforms.helpers import StubVectorContext, make_vector


def test_vector_drop_horizontal_respects_coverage_with_schema():
    stream = iter(
        [
            make_vector(0, {"a": 1.0, "b": 2.0}),
            make_vector(1, {"a": 3.0}),
        ]
    )
    transform = VectorDropTransform(axis="horizontal", threshold=1.0)
    transform.bind_context(StubVectorContext(["a", "b"]))

    out = list(transform.apply(stream))
    assert len(out) == 1
    assert out[0].features.values == {"a": 1.0, "b": 2.0}


def test_vector_drop_horizontal_drops_sparse_vector():
    stream = iter(
        [
            make_vector(0, {"a": 1.0, "b": 2.0}),
            make_vector(1, {"a": 3.0, "b": None}),
        ]
    )
    transform = VectorDropTransform(axis="horizontal", threshold=1.0)
    transform.bind_context(StubVectorContext(["a", "b"]))

    out = list(transform.apply(stream))
    assert len(out) == 1
    assert out[0].features.values == {"a": 1.0, "b": 2.0}


def test_vector_drop_horizontal_sequence_aware():
    stream = iter(
        [
            make_vector(0, {"seq": [1.0, None, None], "f": 2.0}),
        ]
    )
    transform = VectorDropTransform(axis="horizontal", threshold=0.7)
    transform.bind_context(StubVectorContext(["seq", "f"]))

    out = list(transform.apply(stream))
    assert out == []


def test_vector_drop_horizontal_with_only_and_exclude():
    stream = iter([make_vector(0, {"a": 1.0, "b": None, "c": 2.0})])
    transform = VectorDropTransform(
        axis="horizontal",
        threshold=1.0,
        payload="features",
        only=["a", "b"],
        exclude=["b"],
    )
    transform.bind_context(StubVectorContext(["a", "b", "c"]))

    out = list(transform.apply(stream))
    assert len(out) == 1


def test_vector_drop_vertical_delegates_to_partitions():
    stream = iter(
        [
            make_vector(
                0,
                {
                    "humidity__@location:north": 1.0,
                    "humidity__@location:south": 2.0,
                    "temp": 3.0,
                },
            )
        ]
    )
    ctx = StubVectorContext(
        ["humidity__@location:north", "humidity__@location:south", "temp"]
    )
    ctx.set_metadata(
        {
            "counts": {"feature_vectors": 4},
            "features": [
                {"id": "humidity__@location:north", "present_count": 4, "null_count": 3},
                {"id": "humidity__@location:south", "present_count": 4, "null_count": 0},
                {"id": "temp", "present_count": 4, "null_count": 0},
            ],
        }
    )
    transform = VectorDropTransform(axis="vertical", threshold=0.8)
    transform.bind_context(ctx)

    out = list(transform.apply(stream))
    assert list(out[0].features.values.keys()) == [
        "humidity__@location:south",
        "temp",
    ]


def test_vector_drop_partitions_drops_when_coverage_low():
    stream = iter(
        [
            make_vector(
                0,
                {
                    "humidity__@location:north": 1.0,
                    "humidity__@location:south": 2.0,
                    "temp": 3.0,
                },
            )
        ]
    )
    ctx = StubVectorContext(
        ["humidity__@location:north", "humidity__@location:south", "temp"]
    )
    ctx.set_metadata(
        {
            "counts": {"feature_vectors": 4},
            "features": [
                {"id": "humidity__@location:north", "present_count": 4, "null_count": 3},
                {"id": "humidity__@location:south", "present_count": 4, "null_count": 0},
                {"id": "temp", "present_count": 4, "null_count": 0},
            ],
        }
    )
    transform = VectorDropTransform(axis="vertical", threshold=0.8)
    transform.bind_context(ctx)

    ctx.load_schema()
    out = list(transform.apply(stream))
    assert list(out[0].features.values.keys()) == [
        "humidity__@location:south",
        "temp",
    ]
    schema = ctx._cache.get("schema:features") or ctx.load_schema()
    assert [entry["id"] for entry in schema] == [
        "humidity__@location:south",
        "temp",
    ]


def test_vector_drop_partitions_keeps_when_coverage_sufficient():
    stream = iter(
        [
            make_vector(
                0,
                {
                    "precip__@station:north": None,
                    "precip__@station:south": 2.0,
                    "temp": 10.0,
                },
            )
        ]
    )
    ctx = StubVectorContext(
        ["precip__@station:north", "precip__@station:south", "temp"]
    )
    ctx.set_metadata(
        {
            "counts": {"feature_vectors": 4},
            "features": [
                {
                    "id": "precip__@station:north",
                    "present_count": 4,
                    "null_count": 1,
                },
                {
                    "id": "precip__@station:south",
                    "present_count": 4,
                    "null_count": 0,
                },
                {"id": "temp", "present_count": 4, "null_count": 0},
            ],
        }
    )
    transform = VectorDropTransform(axis="vertical", threshold=0.7)
    transform.bind_context(ctx)

    ctx.load_schema()
    out = list(transform.apply(stream))
    assert list(out[0].features.values.keys()) == [
        "precip__@station:north",
        "precip__@station:south",
        "temp",
    ]
    schema = ctx._cache.get("schema:features") or ctx.load_schema()
    assert [entry["id"] for entry in schema] == [
        "precip__@station:north",
        "precip__@station:south",
        "temp",
    ]


def test_vector_drop_vertical_prefers_window_size_for_coverage():
    stream = iter(
        [
            make_vector(
                0,
                {
                    "humidity__@location:north": 1.0,
                },
            )
        ]
    )
    ctx = StubVectorContext(["humidity__@location:north"])
    ctx.set_metadata(
        {
            "window": {"start": "2024-01-01T00:00:00Z", "end": "2024-01-01T04:00:00Z", "size": 5},
            "counts": {"feature_vectors": 100},
            "features": [
                {
                    "id": "humidity__@location:north",
                    "present_count": 3,
                    "null_count": 0,
                },
            ],
        }
    )
    transform = VectorDropTransform(axis="vertical", threshold=0.5)
    transform.bind_context(ctx)

    out = list(transform.apply(stream))
    assert out[0].features is not None
    assert "humidity__@location:north" in out[0].features.values


def test_vector_drop_vertical_uses_element_coverage_for_sequences():
    stream = iter(
        [
            Sample(
                key=(0,),
                features=Vector(
                    values={"pressure__@station_id:06183": [1.0, 2.0, 3.0, None]}
                ),
            )
        ]
    )
    ctx = StubVectorContext(["pressure__@station_id:06183"])
    ctx.set_metadata(
        {
            "window": {"start": "2024-01-01T00:00:00Z", "end": "2024-01-01T01:00:00Z", "size": 1},
            "counts": {"feature_vectors": 1},
            "features": [
                {
                    "id": "pressure__@station_id:06183",
                    "present_count": 1,
                    "null_count": 0,
                    "cadence": {"target": 4},
                    "observed_elements": 3,
                }
            ],
        }
    )
    transform = VectorDropTransform(axis="vertical", threshold=1.0)
    transform.bind_context(ctx)

    out = list(transform.apply(stream))
    assert out and out[0].features is not None
    assert "pressure__@station_id:06183" not in out[0].features.values


def test_vector_drop_partitions_accounts_for_absence():
    stream = iter(
        [
            make_vector(
                0,
                {
                    "wind_speed__@station_id:06052": 1.0,
                },
            )
        ]
    )
    ctx = StubVectorContext(
        ["wind_speed__@station_id:06052", "wind_speed__@station_id:06053"]
    )
    ctx.set_metadata(
        {
            "counts": {"feature_vectors": 4},
            "features": [
                {
                    "id": "wind_speed__@station_id:06052",
                    "present_count": 2,
                    "null_count": 0,
                },
                {
                    "id": "wind_speed__@station_id:06053",
                    "present_count": 4,
                    "null_count": 0,
                },
            ],
        }
    )
    transform = VectorDropTransform(axis="vertical", threshold=0.75)
    transform.bind_context(ctx)

    out = list(transform.apply(stream))
    assert list(out[0].features.values.keys()) == []


def test_vector_drop_partitions_errors_without_metadata():
    stream = iter([make_vector(0, {"temp": 1.0})])
    ctx = StubVectorContext(["temp"])
    transform = VectorDropTransform(axis="vertical", threshold=0.5)
    transform.bind_context(ctx)

    with pytest.raises(RuntimeError):
        list(transform.apply(stream))


def test_vector_drop_vertical_drops_null_target_at_threshold_one():
    stream = iter(
        [
            Sample(
                key=(0,),
                features=Vector(values={}),
                targets=Vector(
                    values={
                        "wind_production__@municipality_no:101": None,
                        "wind_production__@municipality_no:147": 1.0,
                    }
                ),
            )
        ]
    )
    ctx = StubVectorContext(
        {"targets": [
            "wind_production__@municipality_no:101",
            "wind_production__@municipality_no:147",
        ]}
    )
    ctx.set_metadata(
        {
            "counts": {"target_vectors": 1},
            "targets": [
                {
                    "id": "wind_production__@municipality_no:101",
                    "present_count": 1,
                    "null_count": 1,
                },
                {
                    "id": "wind_production__@municipality_no:147",
                    "present_count": 1,
                    "null_count": 0,
                },
            ],
        }
    )
    transform = VectorDropTransform(
        axis="vertical",
        payload="targets",
        threshold=1.0,
    )
    transform.bind_context(ctx)

    out = list(transform.apply(stream))
    assert len(out) == 1
    assert out[0].targets is not None
    assert set(out[0].targets.values.keys()) == {
        "wind_production__@municipality_no:147"
    }


def test_vector_drop_null_feature_level_drops_when_coverage_low():
    stream = iter([make_vector(0, {"a": 1.0, "b": None})])
    transform = VectorDropTransform(axis="horizontal", threshold=0.8)
    transform.bind_context(StubVectorContext(["a", "b"]))

    out = list(transform.apply(stream))
    assert out == []


def test_vector_drop_null_feature_level_keeps_when_coverage_sufficient():
    stream = iter([make_vector(0, {"a": 1.0, "b": 2.0, "c": None})])
    transform = VectorDropTransform(axis="horizontal", threshold=0.5)
    transform.bind_context(StubVectorContext(["a", "b", "c"]))

    out = list(transform.apply(stream))
    assert len(out) == 1


def test_vector_drop_null_record_level_drops_on_sparse_sequence():
    stream = iter([make_vector(0, {"seq": [1.0, None, None], "f": 2.0})])
    transform = VectorDropTransform(axis="horizontal", threshold=0.8)
    transform.bind_context(StubVectorContext(["seq", "f"]))

    out = list(transform.apply(stream))
    assert out == []


def test_vector_drop_null_record_level_ignores_when_no_sequences():
    stream = iter([make_vector(0, {"a": 1.0})])
    transform = VectorDropTransform(axis="horizontal", threshold=0.8)
    transform.bind_context(StubVectorContext(["a"]))

    out = list(transform.apply(stream))
    assert len(out) == 1
