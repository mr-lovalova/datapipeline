from statistics import pstdev, stdev

import pytest

from datapipeline.transforms.stream.rolling import RollingTransformer
from tests.unit.transforms.helpers import make_time_record


def test_rolling_pstdev_matches_statistics_pstdev() -> None:
    stream = iter(
        [
            make_time_record(0.01, 0),
            make_time_record(0.02, 1),
            make_time_record(-0.01, 2),
            make_time_record(0.03, 3),
        ]
    )
    transform = RollingTransformer(
        field="value",
        to="return_pstdev_3",
        window=3,
        statistic="pstdev",
        min_samples=3,
    )

    out = list(transform.apply(stream))

    assert [record.return_pstdev_3 for record in out[:2]] == [None, None]
    assert out[2].return_pstdev_3 == pytest.approx(pstdev([0.01, 0.02, -0.01]))
    assert out[3].return_pstdev_3 == pytest.approx(pstdev([0.02, -0.01, 0.03]))


def test_rolling_stdev_matches_statistics_stdev() -> None:
    stream = iter(
        [
            make_time_record(1.0, 0),
            make_time_record(2.0, 1),
            make_time_record(4.0, 2),
        ]
    )
    transform = RollingTransformer(
        field="value",
        to="sample_stdev_3",
        window=3,
        statistic="stdev",
        min_samples=3,
    )

    out = list(transform.apply(stream))

    assert [record.sample_stdev_3 for record in out[:2]] == [None, None]
    assert out[2].sample_stdev_3 == pytest.approx(stdev([1.0, 2.0, 4.0]))


def test_rolling_stdev_respects_missing_values_and_min_samples() -> None:
    stream = iter(
        [
            make_time_record(1.0, 0),
            make_time_record(None, 1),
            make_time_record(3.0, 2),
        ]
    )
    transform = RollingTransformer(
        field="value",
        to="sample_stdev_3",
        window=3,
        statistic="stdev",
        min_samples=2,
    )

    out = list(transform.apply(stream))

    assert [record.sample_stdev_3 for record in out[:2]] == [None, None]
    assert out[2].sample_stdev_3 == pytest.approx(stdev([1.0, 3.0]))


def test_rolling_max_matches_window_max() -> None:
    stream = iter(
        [
            make_time_record(10.0, 0),
            make_time_record(8.0, 1),
            make_time_record(12.0, 2),
            make_time_record(9.0, 3),
        ]
    )
    transform = RollingTransformer(
        field="value",
        to="rolling_max_3",
        window=3,
        statistic="max",
        min_samples=3,
    )

    out = list(transform.apply(stream))

    assert [record.rolling_max_3 for record in out[:2]] == [None, None]
    assert [record.rolling_max_3 for record in out[2:]] == [12.0, 12.0]


def test_rolling_min_matches_window_min() -> None:
    stream = iter(
        [
            make_time_record(10.0, 0),
            make_time_record(8.0, 1),
            make_time_record(12.0, 2),
            make_time_record(9.0, 3),
        ]
    )
    transform = RollingTransformer(
        field="value",
        to="rolling_min_3",
        window=3,
        statistic="min",
        min_samples=3,
    )

    out = list(transform.apply(stream))

    assert [record.rolling_min_3 for record in out[:2]] == [None, None]
    assert [record.rolling_min_3 for record in out[2:]] == [8.0, 8.0]


def test_rolling_rejects_unknown_statistic_with_supported_names() -> None:
    with pytest.raises(ValueError, match="mean, median, stdev, pstdev, max, min"):
        RollingTransformer(field="value", window=3, statistic="variance")


def test_rolling_stdev_requires_two_samples() -> None:
    with pytest.raises(ValueError, match="min_samples must be at least 2"):
        RollingTransformer(
            field="value",
            window=3,
            statistic="stdev",
            min_samples=1,
        )
