import pytest

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

    transformer = FeatureFill(field="value", method="mean", window=2)

    transformed = list(transformer.apply(stream))
    values = [rec.value for rec in transformed]

    assert values[2] == 11.0  # mean of 10 and 12
    # Window counts ticks, so only the fresh valid value is available.
    assert values[4] == 16.0


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

    transformer = FeatureFill(field="value", window=2)

    transformed = list(transformer.apply(stream))
    values = [rec.value for rec in transformed]

    assert values[3] == 51.0  # median of [100, 2]
    assert values[4] == 2.0  # only the latest tick remains in window


def test_forward_fill_carries_last_valid_value():
    stream = iter(
        [
            make_time_record(None, 0),
            make_time_record(10.0, 1),
            make_time_record(None, 2),
            make_time_record(12.0, 3),
            make_time_record(float("nan"), 4),
        ]
    )

    transformer = FeatureFill(field="value", method="forward")

    transformed = list(transformer.apply(stream))

    assert [rec.value for rec in transformed] == [None, 10.0, 10.0, 12.0, 12.0]


def test_forward_fill_respects_partitions():
    a0 = make_time_record(10.0, 0)
    setattr(a0, "ticker", "A")
    a1 = make_time_record(None, 1)
    setattr(a1, "ticker", "A")
    b0 = make_time_record(None, 0)
    setattr(b0, "ticker", "B")
    b1 = make_time_record(20.0, 1)
    setattr(b1, "ticker", "B")
    b2 = make_time_record(None, 2)
    setattr(b2, "ticker", "B")

    transformer = FeatureFill(
        field="value",
        method="forward",
        partition_by="ticker",
    )

    transformed = list(transformer.apply(iter([a0, a1, b0, b1, b2])))

    assert [rec.value for rec in transformed] == [10.0, 10.0, None, 20.0, 20.0]


def test_forward_fill_can_write_to_separate_field():
    stream = iter(
        [
            make_time_record(10.0, 0),
            make_time_record(None, 1),
        ]
    )

    transformer = FeatureFill(field="value", to="value_asof", method="forward")

    transformed = list(transformer.apply(stream))

    assert [rec.value for rec in transformed] == [10.0, None]
    assert [rec.value_asof for rec in transformed] == [10.0, 10.0]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"method": "unknown"},
        {"method": "forward", "window": 2},
        {"method": "forward", "min_samples": 1},
        {"method": "mean"},
    ],
)
def test_fill_rejects_invalid_method_options(kwargs):
    with pytest.raises(ValueError):
        FeatureFill(field="value", **kwargs)


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


def test_stream_dedupe_rejects_unknown_options() -> None:
    with pytest.raises(TypeError, match="takes no arguments"):
        FeatureDeduplicateTransform(typo=True)
