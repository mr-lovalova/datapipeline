from collections.abc import Iterator
from datetime import datetime, timezone

import pytest

from datapipeline.domain.variable import VariableRecord, VariableSequence
from datapipeline.domain.vector import Vector
from datapipeline.pipelines.vector.nodes import (
    sample_assemble_stage,
    vector_assemble_stage,
)


def _time(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def _keyed(
    *features: VariableRecord | VariableSequence,
) -> Iterator[tuple[tuple, VariableRecord | VariableSequence]]:
    return iter(((feature.time,), feature) for feature in features)


def test_vector_assembly_groups_variable_records() -> None:
    features = _keyed(
        VariableRecord(id="close", time=_time(1), value=10.0),
        VariableRecord(id="volume", time=_time(1), value=20.0),
    )

    vectors = list(vector_assemble_stage(features))

    assert len(vectors) == 1
    assert vectors[0][1].values == {"close": 10.0, "volume": 20.0}


def test_vector_assembly_uses_precomputed_group_key() -> None:
    feature = VariableRecord(id="close", time=_time(2), value=10.0)

    vectors = list(vector_assemble_stage(iter([(("sample",), feature)])))

    assert vectors[0][0] == ("sample",)


def test_vector_assembly_uses_sequence_time() -> None:
    values = [10.0, 20.0]
    sequence = VariableSequence(
        id="close",
        time=_time(2),
        values=values,
    )

    vectors = list(vector_assemble_stage(_keyed(sequence)))

    assert len(vectors) == 1
    assert vectors[0][0] == (_time(2),)
    assert vectors[0][1].values == {"close": [10.0, 20.0]}
    assert vectors[0][1].values["close"] is not values


def test_vector_assembly_preserves_single_item_sequence_shape() -> None:
    sequence = VariableSequence(
        id="close",
        time=_time(1),
        values=[10.0],
    )

    vectors = list(vector_assemble_stage(_keyed(sequence)))

    assert vectors[0][1].values == {"close": [10.0]}


@pytest.mark.parametrize("sequence_first", [False, True])
def test_vector_assembly_rejects_mixed_scalar_and_sequence_values(
    sequence_first: bool,
) -> None:
    record = VariableRecord(id="close", time=_time(1), value=10.0)
    sequence = VariableSequence(
        id="close",
        time=_time(1),
        values=[10.0],
    )
    features = (sequence, record) if sequence_first else (record, sequence)

    with pytest.raises(ValueError, match="both scalar and sequence"):
        list(vector_assemble_stage(_keyed(*features)))


def test_sample_assembly_outputs_samples() -> None:
    feature_vectors = iter(
        [
            (("a",), Vector(values={"close": 10.0})),
            (("b",), Vector(values={"close": 20.0})),
        ]
    )

    samples = list(sample_assemble_stage(feature_vectors))

    assert [sample.key for sample in samples] == [("a",), ("b",)]
