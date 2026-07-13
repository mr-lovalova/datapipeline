import pytest

from datapipeline.sources.synthetic.time.loader import TimeTicksGenerator


@pytest.mark.parametrize("frequency", ["0m", "-1h"])
def test_time_ticks_rejects_nonpositive_frequency(frequency: str) -> None:
    with pytest.raises(ValueError, match="frequency must be positive"):
        TimeTicksGenerator(
            start="2025-01-01T00:00:00Z",
            end="2025-01-01T01:00:00Z",
            frequency=frequency,
        )


def test_time_ticks_accepts_minute_alias() -> None:
    generator = TimeTicksGenerator(
        start="2025-01-01T00:00:00Z",
        end="2025-01-01T00:20:00Z",
        frequency="10min",
    )

    assert [row["time"].minute for row in generator.generate()] == [0, 10, 20]


def test_time_ticks_info_formats_timestamps_as_utc() -> None:
    generator = TimeTicksGenerator(
        start="2025-01-01T00:00:00+02:00",
        end="2025-01-01T01:00:00+02:00",
        frequency="1h",
    )

    assert generator.info_lines() == [
        "synthetic.generate: start=2024-12-31T22:00:00Z "
        "end=2024-12-31T23:00:00Z freq=1h"
    ]
