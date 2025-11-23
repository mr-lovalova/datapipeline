from __future__ import annotations

from datetime import datetime, timezone
from math import isclose
from typing import Any

import pytest
from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.feature.scaler import StandardScaler, StandardScalerTransform
from datapipeline.transforms.stream.dedupe import FeatureDeduplicateTransform
from datapipeline.transforms.stream.fill import FillTransformer as FeatureFill
from datapipeline.transforms.vector import (
    VectorDropMissingTransform,
    VectorEnsureSchemaTransform,
    VectorFillAcrossPartitionsTransform,
    VectorFillConstantTransform,
    VectorFillHistoryTransform,
)


def _make_time_record(value: float, hour: int) -> TemporalRecord:
    return TemporalRecord(
        time=datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc),
        value=value,
    )


def _make_feature_record(value: float, hour: int, feature_id: str) -> FeatureRecord:
    return FeatureRecord(
        record=_make_time_record(value, hour),
        id=feature_id,
    )


def _make_vector(group: int, values: dict[str, Any]) -> Sample:
    return Sample(key=(group,), features=Vector(values=values))


class _StubVectorContext:
    def __init__(
        self,
        expected: list[str] | dict[str, list[str]],
        *,
        schema: dict[str, list[dict]] | None = None,
    ):
        if isinstance(expected, dict):
            self._expected_map = {k: list(v) for k, v in expected.items()}
        else:
            self._expected_map = {"features": list(expected)}
        self._schema_map = schema or {}

    def load_expected_ids(self, *, payload: str = "features") -> list[str]:
        return list(self._expected_map.get(payload, []))

    def load_schema(self, *, payload: str = "features") -> list[dict]:
        entries = self._schema_map.get(payload)
        return [dict(item) for item in entries] if entries else []


def test_standard_scaler_normalizes_feature_stream():
    training_vectors = iter(
        [
            Sample(key=(0,), features=Vector(values={"radiation": 1.0})),
            Sample(key=(1,), features=Vector(values={"radiation": 2.0})),
            Sample(key=(2,), features=Vector(values={"radiation": 3.0})),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)
    scaler = StandardScalerTransform(scaler=scaler_model)

    stream = iter(
        [
            _make_feature_record(1.0, 0, "radiation"),
            _make_feature_record(2.0, 1, "radiation"),
            _make_feature_record(3.0, 2, "radiation"),
        ]
    )

    transformed = list(scaler.apply(stream))

    values = [fr.record.value for fr in transformed]
    expected = [-1.22474487, 0.0, 1.22474487]
    for observed, target in zip(values, expected):
        assert isclose(observed, target, rel_tol=1e-6)


def test_standard_scaler_uses_provided_statistics():
    training_vectors = iter(
        [
            Sample(key=(0,), features=Vector(values={"temperature": 0.0})),
            Sample(key=(1,), features=Vector(values={"temperature": 10.0})),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)
    scaler = StandardScalerTransform(scaler=scaler_model)

    stream = iter(
        [
            _make_feature_record(10.0, 0, "temperature"),
            _make_feature_record(11.0, 1, "temperature"),
        ]
    )

    transformed = list(scaler.apply(stream))

    assert [fr.record.value for fr in transformed] == [1.0, 1.2]


def test_standard_scaler_fit_and_serialize(tmp_path):
    vectors = iter(
        [
            Sample(key=(0,), features=Vector(values={"temp": 10.0, "wind": 5.0})),
            Sample(key=(1,), features=Vector(values={"temp": 14.0, "wind": 7.0})),
        ]
    )

    scaler = StandardScaler()
    total = scaler.fit(vectors)
    assert total == 4
    path = tmp_path / "scaler.pkl"
    scaler.save(path)
    restored = StandardScaler.load(path)
    transform = StandardScalerTransform(model_path=path)

    stream = iter(
        [
            _make_feature_record(12.0, 0, "temp"),
            _make_feature_record(6.0, 0, "wind"),
        ]
    )

    transformed = list(transform.apply(stream))
    assert len(restored.statistics) == 2
    assert len(transformed) == 2


def test_standard_scaler_errors_on_missing_by_default():
    training_vectors = iter(
        [
            Sample(key=(0,), features=Vector(values={"temp": 10.0})),
            Sample(key=(1,), features=Vector(values={"temp": 14.0})),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)
    transform = StandardScalerTransform(scaler=scaler_model)

    stream = iter(
        [
            _make_feature_record(10.0, 0, "temp"),
            _make_feature_record(None, 1, "temp"),
        ]
    )

    with pytest.raises(TypeError):
        list(transform.apply(stream))


def test_standard_scaler_passthrough_missing_counts():
    training_vectors = iter(
        [
            Sample(key=(0,), features=Vector(values={"temp": 1.0})),
            Sample(key=(1,), features=Vector(values={"temp": 3.0})),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)

    transform = StandardScalerTransform(
        scaler=scaler_model, on_none="warn"
    )

    stream = iter(
        [
            _make_feature_record(1.0, 0, "temp"),
            _make_feature_record(None, 1, "temp"),
            _make_feature_record(3.0, 2, "temp"),
        ]
    )

    transformed = list(transform.apply(stream))
    values = [fr.record.value for fr in transformed]
    assert values == [-1.0, None, 1.0]
    assert transform.missing_counts == {"temp": 1}


def test_standard_scaler_warn_callback_invoked_with_counts():
    training_vectors = iter(
        [
            Sample(key=(0,), features=Vector(values={"temp": 1.0})),
            Sample(key=(1,), features=Vector(values={"temp": 3.0})),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)

    calls: list[tuple[str, float | None, int]] = []

    def on_none_cb(event):
        if event.type == "scaler_none":
            calls.append(
                (
                    event.payload["feature_id"],
                    event.payload["record"].time.hour,
                    event.payload["count"],
                )
            )
        elif event.type == "scaler_none_summary":
            calls.append(("summary", None, event.payload["count"]))

    transform = StandardScalerTransform(
        scaler=scaler_model, on_none="warn"
    )
    transform.set_observer(on_none_cb)

    stream = iter(
        [
            _make_feature_record(1.0, 0, "temp"),
            _make_feature_record(None, 1, "temp"),
            _make_feature_record(None, 2, "temp"),
        ]
    )

    transformed = list(transform.apply(stream))

    assert [fr.record.value for fr in transformed] == [-1.0, None, None]
    assert transform.missing_counts == {"temp": 2}
    assert calls == [("temp", 1, 1), ("temp", 2, 2)]


def test_time_mean_fill_uses_running_average():
    stream = iter(
        [
            _make_feature_record(10.0, 0, "temp"),
            _make_feature_record(12.0, 1, "temp"),
            _make_feature_record(None, 2, "temp"),
            _make_feature_record(16.0, 3, "temp"),
            _make_feature_record(float("nan"), 4, "temp"),
        ]
    )

    transformer = FeatureFill(statistic="mean", window=2)

    transformed = list(transformer.apply(stream))
    values = [fr.record.value for fr in transformed]

    assert values[2] == 11.0  # mean of 10 and 12
    assert values[4] == 16.0  # window counts ticks, so only the fresh valid value is available


def test_time_median_fill_honours_window():
    stream = iter(
        [
            _make_feature_record(1.0, 0, "wind"),
            _make_feature_record(100.0, 1, "wind"),
            _make_feature_record(2.0, 2, "wind"),
            _make_feature_record(None, 3, "wind"),
            _make_feature_record(None, 4, "wind"),
        ]
    )

    transformer = FeatureFill(statistic="median", window=2)

    transformed = list(transformer.apply(stream))
    values = [fr.record.value for fr in transformed]

    # history window restricted to last two valid values -> [100, 2]
    assert values[3] == 51.0
    # second missing only sees one valid tick (2.0) because window tracks ticks, not values
    assert values[4] == 2.0


def test_stream_dedupe_removes_exact_duplicates():
    stream = iter(
        [
            _make_feature_record(10.0, 0, "temp"),
            _make_feature_record(10.0, 0, "temp"),
            _make_feature_record(12.0, 1, "temp"),
            _make_feature_record(5.0, 0, "wind"),
            _make_feature_record(5.0, 0, "wind"),
        ]
    )
    transform = FeatureDeduplicateTransform()
    out = list(transform.apply(stream))
    assert [fr.record.value for fr in out] == [10.0, 12.0, 5.0]


def test_stream_dedupe_keeps_distinct_values():
    stream = iter(
        [
            _make_feature_record(10.0, 0, "temp"),
            _make_feature_record(11.0, 0, "temp"),
        ]
    )
    transform = FeatureDeduplicateTransform()
    out = list(transform.apply(stream))
    assert [fr.record.value for fr in out] == [10.0, 11.0]


def test_vector_fill_history_uses_running_statistics():
    stream = iter(
        [
            _make_vector(0, {"temp__A": 10.0}),
            _make_vector(1, {"temp__A": 12.0}),
            _make_vector(2, {}),
        ]
    )

    transform = VectorFillHistoryTransform(
        statistic="mean", window=2, min_samples=2)
    transform.bind_context(_StubVectorContext(["temp__A"]))

    out = list(transform.apply(stream))
    assert out[2].features.values["temp__A"] == 11.0


def test_vector_fill_horizontal_averages_siblings():
    stream = iter(
        [
            _make_vector(0, {"wind__A": 10.0, "wind__B": 14.0}),
            _make_vector(1, {"wind__A": 12.0}),
        ]
    )

    transform = VectorFillAcrossPartitionsTransform(
        statistic="median")
    transform.bind_context(_StubVectorContext(["wind__A", "wind__B"]))

    out = list(transform.apply(stream))
    # First bucket remains unchanged
    assert out[0].features.values == {"wind__A": 10.0, "wind__B": 14.0}
    # Second bucket fills missing wind__B using value from same timestamp (only A present -> not enough samples)
    # Wait we need at least min_samples=1 -> default 1 so fill uses available values
    assert out[1].features.values["wind__B"] == 12.0


def test_vector_fill_constant_injects_value():
    stream = iter([_make_vector(0, {"time": 1.0})])
    transform = VectorFillConstantTransform(value=0.0)
    transform.bind_context(_StubVectorContext(["time", "wind"]))
    out = list(transform.apply(stream))
    assert out[0].features.values["wind"] == 0.0


def test_vector_drop_missing_respects_coverage():
    stream = iter(
        [
            _make_vector(0, {"a": 1.0, "b": 2.0}),
            _make_vector(1, {"a": 3.0}),
        ]
    )

    transform = VectorDropMissingTransform(min_coverage=1.0)
    transform.bind_context(_StubVectorContext(["a", "b"]))

    out = list(transform.apply(stream))
    assert len(out) == 1
    assert out[0].features.values == {"a": 1.0, "b": 2.0}


def test_vector_fill_constant_targets_payload():
    sample = Sample(
        key=(0,),
        features=Vector(values={"f": 1.0}),
        targets=Vector(values={"t": None}),
    )
    transform = VectorFillConstantTransform(value=5.0, payload="targets")
    transform.bind_context(_StubVectorContext({"targets": ["t"]}))

    out = list(transform.apply(iter([sample])))

    assert out[0].features.values == {"f": 1.0}
    assert out[0].targets is not None
    assert out[0].targets.values["t"] == 5.0


def test_vector_drop_missing_targets_payload():
    sample = Sample(
        key=(0,),
        features=Vector(values={"f": 1.0}),
        targets=Vector(values={"t": None}),
    )
    transform = VectorDropMissingTransform(required=["t"], payload="targets")
    transform.bind_context(_StubVectorContext({"targets": ["t"]}))

    assert list(transform.apply(iter([sample]))) == []


def test_vector_ensure_schema_errors_on_missing_by_default():
    stream = iter([_make_vector(0, {"wind__A": 1.0})])
    transform = VectorEnsureSchemaTransform()
    transform.bind_context(
        _StubVectorContext(
            [],
            schema={"features": [{"id": "wind__A"}, {"id": "wind__B"}]},
        )
    )

    with pytest.raises(ValueError):
        list(transform.apply(stream))


def test_vector_ensure_schema_fill_and_reorder():
    stream = iter([_make_vector(0, {"wind__B": 5.0, "wind__A": 2.0, "new": 9.0})])
    transform = VectorEnsureSchemaTransform(
        on_missing="fill", fill_value=0.0, on_extra="drop"
    )
    transform.bind_context(
        _StubVectorContext(
            [],
            schema={"features": [{"id": "wind__A"}, {"id": "wind__B"}]},
        )
    )

    out = list(transform.apply(stream))
    assert out[0].features.values == {"wind__A": 2.0, "wind__B": 5.0}


def test_vector_ensure_schema_inserts_none_for_missing_slots():
    stream = iter([_make_vector(0, {"wind__B": 5.0})])
    transform = VectorEnsureSchemaTransform(on_missing="fill")
    transform.bind_context(
        _StubVectorContext(
            [],
            schema={"features": [{"id": "wind__A"}, {"id": "wind__B"}]},
        )
    )

    out = list(transform.apply(stream))
    assert out[0].features.values == {"wind__A": None, "wind__B": 5.0}


def test_vector_ensure_schema_orders_longer_baseline():
    stream = iter([_make_vector(0, {"wind__C": 7.0, "wind__A": 1.0, "wind__B": 2.0})])
    transform = VectorEnsureSchemaTransform()
    transform.bind_context(
        _StubVectorContext(
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
    assert [*out[0].features.values.keys()] == ["wind__A", "wind__B", "wind__C"]


def test_vector_ensure_schema_drop_sample_on_missing():
    stream = iter([_make_vector(0, {"wind__A": 2.0})])
    transform = VectorEnsureSchemaTransform(on_missing="drop")
    transform.bind_context(
        _StubVectorContext(
            [],
            schema={"features": [{"id": "wind__A"}, {"id": "wind__B"}]},
        )
    )

    out = list(transform.apply(stream))
    assert out == []


def test_vector_ensure_schema_keep_extra_when_allowed():
    stream = iter([_make_vector(0, {"wind__A": 1.0, "wind__B": 2.0, "wind__C": 3.0})])
    transform = VectorEnsureSchemaTransform(on_extra="keep")
    transform.bind_context(
        _StubVectorContext(
            [],
            schema={"features": [{"id": "wind__A"}, {"id": "wind__B"}]},
        )
    )

    out = list(transform.apply(stream))
    assert list(out[0].features.values.keys()) == ["wind__A", "wind__B", "wind__C"]


def test_vector_ensure_schema_prefers_schema_artifact_over_expected_ids():
    stream = iter([_make_vector(0, {"b": 2.0, "a": 1.0})])
    transform = VectorEnsureSchemaTransform()
    transform.bind_context(
        _StubVectorContext(
            [],
            schema={"features": [{"id": "a"}, {"id": "b"}]},
        )
    )

    out = list(transform.apply(stream))
    assert list(out[0].features.values.keys()) == ["a", "b"]


def test_vector_ensure_schema_enforces_list_length_via_schema():
    stream = iter([_make_vector(0, {"seq": [1, 2, 3]})])
    transform = VectorEnsureSchemaTransform(on_missing="fill", fill_value=0)
    transform.bind_context(
        _StubVectorContext(
            [],
            schema={"features": [{"id": "seq", "kind": "list", "cadence": {"target": 5}}]},
        )
    )

    out = list(transform.apply(stream))
    assert out[0].features.values["seq"] == [1, 2, 3, 0, 0]


def test_vector_ensure_schema_enforces_length_error_on_violation():
    stream = iter([_make_vector(0, {"seq": [1, 2, 3, 4]})])
    transform = VectorEnsureSchemaTransform(on_missing="error")
    transform.bind_context(
        _StubVectorContext(
            [],
            schema={"features": [{"id": "seq", "kind": "list", "cadence": {"target": 2}}]},
        )
    )

    with pytest.raises(ValueError):
        list(transform.apply(stream))


def test_vector_ensure_schema_respects_cadence_target_from_schema():
    stream = iter([_make_vector(0, {"seq": [1, 2, 3, 4, 5]})])
    transform = VectorEnsureSchemaTransform(on_missing="fill", fill_value=0)
    transform.bind_context(
        _StubVectorContext(
            [],
            schema={"features": [{"id": "seq", "kind": "list", "cadence": {"target": 6}}]},
        )
    )

    out = list(transform.apply(stream))
    assert len(out[0].features.values["seq"]) == 6


def test_vector_ensure_schema_raises_without_schema_artifact():
    stream = iter([_make_vector(0, {"wind__A": 1.0})])
    transform = VectorEnsureSchemaTransform()
    transform.bind_context(_StubVectorContext(["wind__A"]))

    with pytest.raises(RuntimeError):
        list(transform.apply(stream))
