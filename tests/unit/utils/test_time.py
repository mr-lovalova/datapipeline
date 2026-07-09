from datetime import timedelta

import pytest

from datapipeline.utils.time import parse_cadence, parse_timecode


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("30s", timedelta(seconds=30)),
        ("10m", timedelta(minutes=10)),
        ("10min", timedelta(minutes=10)),
        ("-1h", timedelta(hours=-1)),
        ("+2d", timedelta(days=2)),
    ],
)
def test_parse_timecode(value: str, expected: timedelta) -> None:
    assert parse_timecode(value) == expected


@pytest.mark.parametrize("value", ["", "1", "1w", "1.5h"])
def test_parse_timecode_rejects_unsupported_values(value: str) -> None:
    with pytest.raises(ValueError, match="Unsupported timecode"):
        parse_timecode(value)


def test_parse_cadence_accepts_positive_dataset_units() -> None:
    assert parse_cadence("90min") == timedelta(minutes=90)


@pytest.mark.parametrize("value", ["0m", "-1h", "1s"])
def test_parse_cadence_rejects_values_outside_dataset_contract(value: str) -> None:
    with pytest.raises(ValueError, match="Unsupported cadence"):
        parse_cadence(value)
