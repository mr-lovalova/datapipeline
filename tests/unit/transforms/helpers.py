from datetime import datetime, timezone
from typing import Any

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector


def make_time_record(value: float | None, hour: int) -> TemporalRecord:
    record = TemporalRecord(
        time=datetime(2024, 1, 1, hour=hour, tzinfo=timezone.utc),
    )
    setattr(record, "value", value)
    return record


def make_feature_record(
    value: float | None, hour: int, feature_id: str
) -> FeatureRecord:
    return FeatureRecord(
        record=make_time_record(value, hour),
        id=feature_id,
        value=value,
    )


def make_vector(group: int, values: dict[str, Any]) -> Sample:
    return Sample(key=(group,), features=Vector(values=values))
