from collections.abc import Iterator
from datetime import datetime, timezone

import pytest

from datapipeline.domain.series import SeriesRecord, SeriesSequence
from datapipeline.domain.vector import Vector
from datapipeline.pipelines.sample.assembly import (
    assemble_samples,
    assemble_vectors,
)


def _time(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def _keyed(
    *features: SeriesRecord | SeriesSequence,
) -> Iterator[tuple[tuple, SeriesRecord | SeriesSequence]]:
    return iter(((feature.time,), feature) for feature in features)


def test_assemble_vectors_groups_series() -> None:
    features = _keyed(
        SeriesRecord(id="close", time=_time(1), value=10.0),
        SeriesRecord(id="volume", time=_time(1), value=20.0),
    )

    vectors = list(assemble_vectors(features))

    assert len(vectors) == 1
    assert vectors[0][1].values == {"close": 10.0, "volume": 20.0}


def test_assemble_vectors_uses_precomputed_sample_key() -> None:
    feature = SeriesRecord(id="close", time=_time(2), value=10.0)

    vectors = list(assemble_vectors(iter([(("sample",), feature)])))

    assert vectors[0][0] == ("sample",)


def test_assemble_vectors_uses_sequence_time() -> None:
    values = [10.0, 20.0]
    sequence = SeriesSequence(
        id="close",
        time=_time(2),
        values=values,
    )

    vectors = list(assemble_vectors(_keyed(sequence)))

    assert len(vectors) == 1
    assert vectors[0][0] == (_time(2),)
    assert vectors[0][1].values == {"close": [10.0, 20.0]}
    assert vectors[0][1].values["close"] is not values


def test_assemble_vectors_preserves_single_item_sequence_shape() -> None:
    sequence = SeriesSequence(
        id="close",
        time=_time(1),
        values=[10.0],
    )

    vectors = list(assemble_vectors(_keyed(sequence)))

    assert vectors[0][1].values == {"close": [10.0]}


@pytest.mark.parametrize("sequence_first", [False, True])
def test_assemble_vectors_rejects_mixed_scalar_and_sequence_values(
    sequence_first: bool,
) -> None:
    record = SeriesRecord(id="close", time=_time(1), value=10.0)
    sequence = SeriesSequence(
        id="close",
        time=_time(1),
        values=[10.0],
    )
    features = (sequence, record) if sequence_first else (record, sequence)

    with pytest.raises(ValueError, match="both scalar and sequence"):
        list(assemble_vectors(_keyed(*features)))


def test_assemble_samples_outputs_samples() -> None:
    feature_vectors = iter(
        [
            (("a",), Vector(values={"close": 10.0})),
            (("b",), Vector(values={"close": 20.0})),
        ]
    )

    samples = list(assemble_samples(feature_vectors))

    assert [sample.key for sample in samples] == [("a",), ("b",)]
