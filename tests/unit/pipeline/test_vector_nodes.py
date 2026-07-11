from datetime import datetime, timezone

from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
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


def test_vector_assembly_anchors_sequence_to_last_record_time() -> None:
    sequence = FeatureRecordSequence(
        id="close",
        records=[_record(1), _record(2)],
        values=[10.0, 20.0],
    )

    vectors = list(vector_assemble_stage(iter([sequence]), "1d"))

    assert len(vectors) == 1
    assert vectors[0][0] == (_record(2).time,)
    assert vectors[0][1].values == {"close": [10.0, 20.0]}


def test_sample_assembly_outputs_samples() -> None:
    feature_vectors = iter(
        [
            (("a",), Vector(values={"close": 10.0})),
            (("b",), Vector(values={"close": 20.0})),
        ]
    )

    samples = list(sample_assemble_stage(feature_vectors))

    assert [sample.key for sample in samples] == [("a",), ("b",)]
