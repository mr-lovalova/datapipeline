import pytest

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


def test_vector_ensure_schema_fills_missing_scalar_and_list_slots():
    stream = iter([make_vector(0, {"wind__B": 5.0})])
    transform = VectorEnsureSchemaTransform(on_missing="fill")
    transform.bind_context(
        StubVectorContext(
            [],
            schema={
                "features": [
                    {"id": "wind__A"},
                    {"id": "wind__B"},
                    {"id": "sequence", "kind": "list", "cadence": {"target": 2}},
                ]
            },
        )
    )

    out = list(transform.apply(stream))
    assert out[0].features.values == {
        "wind__A": None,
        "wind__B": 5.0,
        "sequence": [None, None],
    }


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


@pytest.mark.parametrize("values", [{}, {"seq": [1, 2, 3]}])
def test_vector_ensure_schema_drops_missing_ids_and_cadence_violations(values):
    stream = iter([make_vector(0, values)])
    transform = VectorEnsureSchemaTransform(on_missing="drop")
    transform.bind_context(
        StubVectorContext(
            [],
            schema={
                "features": [{"id": "seq", "kind": "list", "cadence": {"target": 2}}]
            },
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


def test_vector_ensure_schema_wraps_scalar_for_length_one_list():
    stream = iter([make_vector(0, {"seq": 1.0})])
    transform = VectorEnsureSchemaTransform(on_missing="fill", fill_value=0)
    transform.bind_context(
        StubVectorContext(
            [],
            schema={
                "features": [{"id": "seq", "kind": "list", "cadence": {"target": 1}}]
            },
        )
    )

    out = list(transform.apply(stream))
    assert out[0].features.values["seq"] == [1.0]


@pytest.mark.parametrize(
    ("value", "target"),
    [([1, 2, 3, 4], 2), (1.0, 1)],
)
def test_vector_ensure_schema_enforces_length_error_on_violation(value, target):
    stream = iter([make_vector(0, {"seq": value})])
    transform = VectorEnsureSchemaTransform(on_missing="error")
    transform.bind_context(
        StubVectorContext(
            [],
            schema={
                "features": [
                    {"id": "seq", "kind": "list", "cadence": {"target": target}}
                ]
            },
        )
    )

    with pytest.raises(ValueError):
        list(transform.apply(stream))


@pytest.mark.parametrize(
    ("target", "expected"),
    [(6, [1, 2, 3, 4, 5, 0]), (3, [1, 2, 3])],
)
def test_vector_ensure_schema_respects_cadence_target_from_schema(target, expected):
    stream = iter([make_vector(0, {"seq": [1, 2, 3, 4, 5]})])
    transform = VectorEnsureSchemaTransform(on_missing="fill", fill_value=0)
    transform.bind_context(
        StubVectorContext(
            [],
            schema={
                "features": [
                    {"id": "seq", "kind": "list", "cadence": {"target": target}}
                ]
            },
        )
    )

    out = list(transform.apply(stream))
    assert out[0].features.values["seq"] == expected


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


def test_vector_ensure_schema_uses_schema_snapshot_captured_at_bind_time():
    stream = iter([make_vector(0, {"wind__B": 5.0, "wind__A": 2.0})])
    context = StubVectorContext(
        [],
        schema={"features": [{"id": "wind__A"}, {"id": "wind__B"}]},
    )
    transform = VectorEnsureSchemaTransform()
    transform.bind_context(context)

    context.remove_schema_ids("features", {"wind__A", "wind__B"})

    out = list(transform.apply(stream))
    assert out[0].features.values == {"wind__A": 2.0, "wind__B": 5.0}


def test_vector_ensure_schema_refreshes_snapshot_when_rebound():
    transform = VectorEnsureSchemaTransform()
    transform.bind_context(StubVectorContext(["old"]))
    transform.bind_context(StubVectorContext(["new"]))

    out = list(transform.apply(iter([make_vector(0, {"new": 1.0})])))

    assert out[0].features.values == {"new": 1.0}
