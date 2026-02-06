import json
from datetime import datetime, timezone

from datapipeline.domain.feature import FeatureRecord
from datapipeline.domain.record import TemporalRecord
from datapipeline.io.serializers import (
    record_json_line_serializer,
    record_csv_row_serializer,
)


def test_record_json_serializer_handles_temporal_record() -> None:
    rec = TemporalRecord(time=datetime(2024, 1, 1, tzinfo=timezone.utc))
    setattr(rec, "value", 42.5)
    serializer = record_json_line_serializer()

    line = serializer(rec)
    payload = json.loads(line)

    assert payload["time"] == "2024-01-01 00:00:00+00:00"
    assert payload["value"] == 42.5


def test_record_csv_serializer_uses_nested_record_time() -> None:
    record = TemporalRecord(time=datetime(2024, 7, 4, tzinfo=timezone.utc))
    setattr(record, "value", 7.0)
    feature = FeatureRecord(id="feature_a", record=record, value=7.0)
    serializer = record_csv_row_serializer()

    key_text, payload_text = serializer(feature)
    payload = json.loads(payload_text)

    assert key_text == "2024-07-04 00:00:00+00:00"
    assert payload["id"] == "feature_a"
    assert payload["record"]["value"] == 7.0
