import pytest

from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector import VectorEnsureSchemaTransform
from tests.unit.transforms.helpers import StubVectorContext, make_vector


def test_vector_ensure_schema_errors_on_missing_by_default():
    stream = iter([make_vector(0, {"wind__A": 1.0})])
    transform = VectorEnsureSchemaTransform()
    transform.bind_context(
        StubVectorContext(
            [],
            schema={"features": [{"id": "wind__A"}, {"id": "wind__B"}]},
        )
    )

    with pytest.raises(ValueError):
        list(transform.apply(stream))


def test_vector_ensure_schema_fill_and_reorder():
    stream = iter([make_vector(0, {"wind__B": 5.0, "wind__A": 2.0, "new": 9.0})])
    transform = VectorEnsureSchemaTransform(on_missing="fill", fill_value=0.0, on_extra="drop")
    transform.bind_context(
        StubVectorContext(
            [],
            schema={"features": [{"id": "wind__A"}, {"id": "wind__B"}]},
        )
    )

    out = list(transform.apply(stream))
    assert out[0].features.values == {"wind__A": 2.0, "wind__B": 5.0}


def test_vector_ensure_schema_inserts_none_for_missing_slots():
    stream = iter([make_vector(0, {"wind__B": 5.0})])
    transform = VectorEnsureSchemaTransform(on_missing="fill")
    transform.bind_context(
        StubVectorContext(
            [],
            schema={"features": [{"id": "wind__A"}, {"id": "wind__B"}]},
        )
    )

    out = list(transform.apply(stream))
    assert out[0].features.values == {"wind__A": None, "wind__B": 5.0}


def test_vector_ensure_schema_orders_longer_baseline():
    stream = iter([make_vector(0, {"wind__C": 7.0, "wind__A": 1.0, "wind__B": 2.0})])
    transform = VectorEnsureSchemaTransform()
    transform.bind_context(
        StubVectorContext(
            [],
            schema={
                "features": [
                    {"id": "wind__A"},
                    {"id": "wind__B"},
                    {"id": "wind__C"},
                ]
            },
        )
    )

    out = list(transform.apply(stream))
    assert list(out[0].features.values.keys()) == ["wind__A", "wind__B", "wind__C"]


def test_vector_ensure_schema_drop_sample_on_missing():
    stream = iter([make_vector(0, {"wind__A": 2.0})])
    transform = VectorEnsureSchemaTransform(on_missing="drop")
    transform.bind_context(
        StubVectorContext(
            [],
            schema={"features": [{"id": "wind__A"}, {"id": "wind__B"}]},
        )
    )

    out = list(transform.apply(stream))
    assert out == []


def test_vector_ensure_schema_keep_extra_when_allowed():
    stream = iter([make_vector(0, {"wind__A": 1.0, "wind__B": 2.0, "wind__C": 3.0})])
    transform = VectorEnsureSchemaTransform(on_extra="keep")
    transform.bind_context(
        StubVectorContext(
            [],
            schema={"features": [{"id": "wind__A"}, {"id": "wind__B"}]},
        )
    )

    out = list(transform.apply(stream))
    assert list(out[0].features.values.keys()) == ["wind__A", "wind__B", "wind__C"]


def test_vector_ensure_schema_prefers_schema_artifact_over_expected_ids():
    stream = iter([make_vector(0, {"b": 2.0, "a": 1.0})])
    transform = VectorEnsureSchemaTransform()
    transform.bind_context(
        StubVectorContext(
            [],
            schema={"features": [{"id": "a"}, {"id": "b"}]},
        )
    )

    out = list(transform.apply(stream))
    assert list(out[0].features.values.keys()) == ["a", "b"]


def test_vector_ensure_schema_enforces_list_length_via_schema():
    stream = iter([make_vector(0, {"seq": [1, 2, 3]})])
    transform = VectorEnsureSchemaTransform(on_missing="fill", fill_value=0)
    transform.bind_context(
        StubVectorContext(
            [],
            schema={"features": [{"id": "seq", "kind": "list", "cadence": {"target": 5}}]},
        )
    )

    out = list(transform.apply(stream))
    assert out[0].features.values["seq"] == [1, 2, 3, 0, 0]


def test_vector_ensure_schema_enforces_length_error_on_violation():
    stream = iter([make_vector(0, {"seq": [1, 2, 3, 4]})])
    transform = VectorEnsureSchemaTransform(on_missing="error")
    transform.bind_context(
        StubVectorContext(
            [],
            schema={"features": [{"id": "seq", "kind": "list", "cadence": {"target": 2}}]},
        )
    )

    with pytest.raises(ValueError):
        list(transform.apply(stream))


def test_vector_ensure_schema_respects_cadence_target_from_schema():
    stream = iter([make_vector(0, {"seq": [1, 2, 3, 4, 5]})])
    transform = VectorEnsureSchemaTransform(on_missing="fill", fill_value=0)
    transform.bind_context(
        StubVectorContext(
            [],
            schema={"features": [{"id": "seq", "kind": "list", "cadence": {"target": 6}}]},
        )
    )

    out = list(transform.apply(stream))
    assert len(out[0].features.values["seq"]) == 6


def test_vector_ensure_schema_raises_without_schema_artifact():
    stream = iter([make_vector(0, {"wind__A": 1.0})])
    transform = VectorEnsureSchemaTransform()
    transform.bind_context(
        StubVectorContext(
            [],
            schema={"features": []},
        )
    )

    with pytest.raises(RuntimeError):
        list(transform.apply(stream))
