from datetime import datetime, timezone

import pytest

from datapipeline.domain.feature import FeatureRecord, FeatureSequence
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.vector import Vector
from datapipeline.pipelines.vector.nodes import (
    sample_assemble_stage,
    vector_assemble_stage,
)


def _record(day: int) -> TemporalRecord:
    return TemporalRecord(datetime(2024, 1, day, tzinfo=timezone.utc))


def test_vector_assembly_groups_feature_records() -> None:
    features = iter(
        [
            FeatureRecord(id="close", record=_record(1), value=10.0),
            FeatureRecord(id="volume", record=_record(1), value=20.0),
        ]
    )

    vectors = list(vector_assemble_stage(features, "1d"))

    assert len(vectors) == 1
    assert vectors[0][1].values == {"close": 10.0, "volume": 20.0}


def test_vector_assembly_uses_sequence_time() -> None:
    values = [10.0, 20.0]
    sequence = FeatureSequence(
        id="close",
        time=_record(2).time,
        values=values,
    )

    vectors = list(vector_assemble_stage(iter([sequence]), "1d"))

    assert len(vectors) == 1
    assert vectors[0][0] == (_record(2).time,)
    assert vectors[0][1].values == {"close": [10.0, 20.0]}
    assert vectors[0][1].values["close"] is not values


def test_vector_assembly_preserves_single_item_sequence_shape() -> None:
    sequence = FeatureSequence(
        id="close",
        time=_record(1).time,
        values=[10.0],
    )

    vectors = list(vector_assemble_stage(iter([sequence]), "1d"))

    assert vectors[0][1].values == {"close": [10.0]}


@pytest.mark.parametrize("sequence_first", [False, True])
def test_vector_assembly_rejects_mixed_scalar_and_sequence_values(
    sequence_first: bool,
) -> None:
    record = FeatureRecord(id="close", record=_record(1), value=10.0)
    sequence = FeatureSequence(
        id="close",
        time=_record(1).time,
        values=[10.0],
    )
    features = [sequence, record] if sequence_first else [record, sequence]

    with pytest.raises(ValueError, match="both scalar and sequence"):
        list(vector_assemble_stage(iter(features), "1d"))


def test_sample_assembly_outputs_samples() -> None:
    feature_vectors = iter(
        [
            (("a",), Vector(values={"close": 10.0})),
            (("b",), Vector(values={"close": 20.0})),
        ]
    )

    samples = list(sample_assemble_stage(feature_vectors))

    assert [sample.key for sample in samples] == [("a",), ("b",)]
