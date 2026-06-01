from datetime import datetime, timezone

from datapipeline.domain.feature import FeatureRecordSequence
from datapipeline.domain.record import TemporalRecord
from datapipeline.pipelines.vector.nodes import vector_assemble_stage


def _record(day: int) -> TemporalRecord:
    return TemporalRecord(datetime(2024, 1, day, tzinfo=timezone.utc))


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
