import pytest

from datapipeline.sources.models.generator import DataGenerator
from datapipeline.sources.models.loader import SyntheticLoader
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


def test_time_ticks_rejects_reversed_bounds() -> None:
    with pytest.raises(ValueError, match="end must not precede start"):
        TimeTicksGenerator(
            start="2025-01-02T00:00:00Z",
            end="2025-01-01T00:00:00Z",
        )


def test_time_ticks_count_matches_generated_rows() -> None:
    generator = TimeTicksGenerator(
        start="2025-01-01T00:00:00+02:00",
        end="2025-01-01T01:00:00+02:00",
        frequency="1h",
    )

    assert generator.count() == len(list(generator)) == 2


def test_synthetic_loader_propagates_generator_count_errors() -> None:
    class BrokenGenerator(DataGenerator):
        def generate(self):
            yield from ()

        def count(self) -> int:
            raise RuntimeError("count failed")

    loader = SyntheticLoader(BrokenGenerator())

    with pytest.raises(RuntimeError, match="count failed"):
        loader.count()
