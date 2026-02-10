from math import isclose

import pytest

from datapipeline.transforms.feature.scaler import StandardScaler, StandardScalerTransform
from tests.unit.transforms.helpers import make_feature_record, make_vector


def test_standard_scaler_normalizes_feature_stream():
    training_vectors = iter(
        [
            make_vector(0, {"radiation": 1.0}),
            make_vector(1, {"radiation": 2.0}),
            make_vector(2, {"radiation": 3.0}),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)
    scaler = StandardScalerTransform(scaler=scaler_model)

    stream = iter(
        [
            make_feature_record(1.0, 0, "radiation"),
            make_feature_record(2.0, 1, "radiation"),
            make_feature_record(3.0, 2, "radiation"),
        ]
    )

    transformed = list(scaler.apply(stream))

    values = [fr.value for fr in transformed]
    expected = [-1.22474487, 0.0, 1.22474487]
    for observed, target in zip(values, expected):
        assert isclose(observed, target, rel_tol=1e-6)


def test_standard_scaler_uses_provided_statistics():
    training_vectors = iter(
        [
            make_vector(0, {"temperature": 0.0}),
            make_vector(1, {"temperature": 10.0}),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)
    scaler = StandardScalerTransform(scaler=scaler_model)

    stream = iter(
        [
            make_feature_record(10.0, 0, "temperature"),
            make_feature_record(11.0, 1, "temperature"),
        ]
    )

    transformed = list(scaler.apply(stream))

    assert [fr.value for fr in transformed] == [1.0, 1.2]


def test_standard_scaler_inverse_transform_round_trip():
    training_vectors = iter(
        [
            make_vector(0, {"radiation": 1.0}),
            make_vector(1, {"radiation": 2.0}),
            make_vector(2, {"radiation": 3.0}),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)
    transform = StandardScalerTransform(scaler=scaler_model)

    stream = iter(
        [
            make_feature_record(1.0, 0, "radiation"),
            make_feature_record(2.0, 1, "radiation"),
            make_feature_record(3.0, 2, "radiation"),
        ]
    )

    scaled = list(transform.apply(stream))
    restored = list(transform.inverse(iter(scaled)))

    original = [1.0, 2.0, 3.0]
    values = [fr.value for fr in restored]
    for observed, expected in zip(values, original):
        assert isclose(observed, expected, rel_tol=1e-6)


def test_standard_scaler_fit_and_serialize(tmp_path):
    vectors = iter(
        [
            make_vector(0, {"temp": 10.0, "wind": 5.0}),
            make_vector(1, {"temp": 14.0, "wind": 7.0}),
        ]
    )

    scaler = StandardScaler()
    total = scaler.fit(vectors)
    assert total == 4
    path = tmp_path / "scaler.json"
    scaler.save(path)
    restored = StandardScaler.load(path)
    transform = StandardScalerTransform(model_path=path)

    stream = iter(
        [
            make_feature_record(12.0, 0, "temp"),
            make_feature_record(6.0, 0, "wind"),
        ]
    )

    transformed = list(transform.apply(stream))
    assert len(restored.statistics) == 2
    assert len(transformed) == 2


def test_standard_scaler_errors_on_missing_by_default():
    training_vectors = iter(
        [
            make_vector(0, {"temp": 10.0}),
            make_vector(1, {"temp": 14.0}),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)
    transform = StandardScalerTransform(scaler=scaler_model, on_none="error")

    stream = iter(
        [
            make_feature_record(10.0, 0, "temp"),
            make_feature_record(None, 1, "temp"),
        ]
    )

    with pytest.raises(TypeError):
        list(transform.apply(stream))


def test_standard_scaler_passthrough_missing_counts():
    training_vectors = iter(
        [
            make_vector(0, {"temp": 1.0}),
            make_vector(1, {"temp": 3.0}),
        ]
    )
    scaler_model = StandardScaler()
    scaler_model.fit(training_vectors)

    transform = StandardScalerTransform(scaler=scaler_model, on_none="skip")

    stream = iter(
        [
            make_feature_record(1.0, 0, "temp"),
            make_feature_record(None, 1, "temp"),
            make_feature_record(3.0, 2, "temp"),
        ]
    )

    transformed = list(transform.apply(stream))
    values = [fr.value for fr in transformed]
    assert values == [-1.0, None, 1.0]
    assert transform.missing_counts == {"temp": 1}


def test_standard_scaler_matches_sklearn():
    sklearn = pytest.importorskip("sklearn.preprocessing")
    SKStandardScaler = getattr(sklearn, "StandardScaler")

    values = [1.0, 2.0, 3.0, 4.0]
    vectors = iter(
        [make_vector(i, {"x": v}) for i, v in enumerate(values)]
    )

    scaler = StandardScaler()
    scaler.fit(vectors)

    sk_scaler = SKStandardScaler()
    sk_scaler.fit([[v] for v in values])

    stream = iter([make_feature_record(v, i, "x") for i, v in enumerate(values)])
    transformed = list(StandardScalerTransform(scaler=scaler).apply(stream))
    ours = [fr.value for fr in transformed]
    theirs = sk_scaler.transform([[v] for v in values]).flatten().tolist()

    assert pytest.approx(ours) == theirs


def test_standard_scaler_warn_callback_invoked_with_counts():
    training_vectors = iter(
        [
            make_vector(0, {"temp": 1.0}),
            make_vector(1, {"temp": 3.0}),
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

    transform = StandardScalerTransform(scaler=scaler_model, on_none="skip")
    transform.set_observer(on_none_cb)

    stream = iter(
        [
            make_feature_record(1.0, 0, "temp"),
            make_feature_record(None, 1, "temp"),
            make_feature_record(None, 2, "temp"),
        ]
    )

    transformed = list(transform.apply(stream))

    assert [fr.value for fr in transformed] == [-1.0, None, None]
    assert transform.missing_counts == {"temp": 2}
    assert calls == [("temp", 1, 1), ("temp", 2, 2)]
