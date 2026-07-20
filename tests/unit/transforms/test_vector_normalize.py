import pytest

from datapipeline.artifacts.models import (
    ListVectorMetadataEntry,
    ScalarVectorMetadataEntry,
    VectorMetadataEntry,
)
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.transforms.vector.normalize import (
    NormalizeFeaturesTransform,
    NormalizeTargetsTransform,
)
from tests.unit.transforms.helpers import make_vector


def _scalar(identifier: str) -> VectorMetadataEntry:
    return ScalarVectorMetadataEntry(
        id=identifier,
        base_id=identifier,
        kind="scalar",
        present_count=0,
        null_count=0,
    )


def _sequence(identifier: str, size: int) -> VectorMetadataEntry:
    return ListVectorMetadataEntry(
        id=identifier,
        base_id=identifier,
        kind="list",
        present_count=0,
        null_count=0,
        length=size,
        observed_elements=0,
    )


def test_normalize_features_fills_missing_values_and_orders_schema() -> None:
    transform = NormalizeFeaturesTransform(
        [_scalar("first"), _sequence("history", 2), _scalar("last")]
    )

    output = list(transform.apply(iter([make_vector(0, {"last": 3.0})])))

    assert output[0].features.values == {
        "first": None,
        "history": [None, None],
        "last": 3.0,
    }


def test_normalize_features_reuses_validated_sequence() -> None:
    history = [1.0, 2.0]
    sample = make_vector(0, {"history": history})

    [output] = NormalizeFeaturesTransform([_sequence("history", 2)]).apply(
        iter([sample])
    )

    assert output.features.values["history"] is history


def test_normalize_features_rejects_unknown_ids() -> None:
    transform = NormalizeFeaturesTransform([_scalar("known")])

    with pytest.raises(ValueError, match="unexpected ids"):
        list(transform.apply(iter([make_vector(0, {"known": 1.0, "extra": 2.0})])))


@pytest.mark.parametrize(
    "value",
    [[1.0], [1.0, 2.0, 3.0], 1.0],
)
def test_normalize_features_rejects_wrong_sequence_shape(value: object) -> None:
    transform = NormalizeFeaturesTransform([_sequence("history", 2)])

    with pytest.raises((TypeError, ValueError)):
        list(transform.apply(iter([make_vector(0, {"history": value})])))


@pytest.mark.parametrize("value", ["category", float("inf")])
def test_normalize_features_preserves_scalar_values(value: object) -> None:
    transform = NormalizeFeaturesTransform([_scalar("value")])

    [output] = transform.apply(iter([make_vector(0, {"value": value})]))

    assert output.features.values == {"value": value}


def test_normalize_targets_creates_missing_target_vector() -> None:
    sample = make_vector(0, {"feature": 1.0})
    transform = NormalizeTargetsTransform([_scalar("target")])

    output = list(transform.apply(iter([sample])))

    assert output[0].features.values == {"feature": 1.0}
    assert output[0].targets is not None
    assert output[0].targets.values == {"target": None}


def test_normalize_targets_preserves_feature_vector() -> None:
    sample = Sample(
        key=(0,),
        features=Vector(values={"feature": 1.0}),
        targets=Vector(values={"target": 2.0}),
    )
    transform = NormalizeTargetsTransform([_scalar("target")])

    output = list(transform.apply(iter([sample])))

    assert output[0].features is sample.features
    assert output[0].targets is not None
    assert output[0].targets.values == {"target": 2.0}


def test_normalize_targets_fills_and_orders_scalar_and_sequence_entries() -> None:
    sample = Sample(
        key=(0,),
        features=Vector(values={"feature": 1.0}),
        targets=Vector(values={"last": 3.0, "history": [1.0, 2.0]}),
    )
    transform = NormalizeTargetsTransform(
        [_scalar("first"), _sequence("history", 2), _scalar("last")]
    )

    [output] = transform.apply(iter([sample]))

    assert output.targets is not None
    assert list(output.targets.values) == ["first", "history", "last"]
    assert output.targets.values == {
        "first": None,
        "history": [1.0, 2.0],
        "last": 3.0,
    }
