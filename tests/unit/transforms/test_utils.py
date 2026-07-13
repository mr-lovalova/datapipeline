import pytest

from datapipeline.transforms.utils import partition_key
from tests.unit.transforms.helpers import make_time_record


def test_partition_key_requires_declared_mapping_field() -> None:
    with pytest.raises(KeyError, match="Partition field 'security_id'"):
        partition_key({"value": 1.0}, "security_id")


def test_partition_key_requires_declared_object_field() -> None:
    record = make_time_record(1.0, 0)

    with pytest.raises(KeyError, match="Partition field 'security_id'"):
        partition_key(record, "security_id")


def test_partition_key_preserves_present_none_value() -> None:
    record = make_time_record(1.0, 0)
    record.security_id = None

    assert partition_key(record, "security_id") == (None,)


def test_partition_key_reads_multiple_fields_in_declared_order() -> None:
    record = make_time_record(1.0, 0)
    record.security_id = "AAPL"
    record.venue = "XNAS"

    assert partition_key(record, ["security_id", "venue"]) == ("AAPL", "XNAS")
