from __future__ import annotations

from datapipeline.transforms.stream.dedupe import FeatureDeduplicateTransform
from datapipeline.transforms.stream.fill import FillTransformer as FeatureFill
from tests.unit.transforms.helpers import make_time_record


def test_time_mean_fill_uses_running_average():
    stream = iter(
        [
            make_time_record(10.0, 0),
            make_time_record(12.0, 1),
            make_time_record(None, 2),
            make_time_record(16.0, 3),
            make_time_record(float("nan"), 4),
        ]
    )

    transformer = FeatureFill(field="value", statistic="mean", window=2)

    transformed = list(transformer.apply(stream))
    values = [rec.value for rec in transformed]

    assert values[2] == 11.0  # mean of 10 and 12
    assert values[4] == 16.0  # window counts ticks, so only the fresh valid value is available


def test_time_median_fill_honours_window():
    stream = iter(
        [
            make_time_record(1.0, 0),
            make_time_record(100.0, 1),
            make_time_record(2.0, 2),
            make_time_record(None, 3),
            make_time_record(None, 4),
        ]
    )

    transformer = FeatureFill(field="value", statistic="median", window=2)

    transformed = list(transformer.apply(stream))
    values = [rec.value for rec in transformed]

    assert values[3] == 51.0  # median of [100, 2]
    assert values[4] == 2.0  # only the latest tick remains in window


def test_stream_dedupe_removes_exact_duplicates():
    stream = iter(
        [
            make_time_record(10.0, 0),
            make_time_record(10.0, 0),
            make_time_record(12.0, 1),
            make_time_record(5.0, 0),
            make_time_record(5.0, 0),
        ]
    )
    transform = FeatureDeduplicateTransform()
    out = list(transform.apply(stream))
    assert [rec.value for rec in out] == [10.0, 12.0, 5.0]


def test_stream_dedupe_keeps_distinct_values():
    stream = iter(
        [
            make_time_record(10.0, 0),
            make_time_record(11.0, 0),
        ]
    )
    transform = FeatureDeduplicateTransform()
    out = list(transform.apply(stream))
    assert [rec.value for rec in out] == [10.0, 11.0]
