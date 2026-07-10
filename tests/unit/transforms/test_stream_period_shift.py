from datetime import timedelta

import pytest

from datapipeline.transforms.record.shift_time import ShiftTimeRecordTransform
from datapipeline.transforms.engine import apply_transforms
from datapipeline.transforms.spec import TransformSpec
from datapipeline.transforms.stream.lag import LagTransformer
from datapipeline.transforms.stream.lead import LeadTransformer
from datapipeline.plugins import STREAM_TRANFORMS_EP
from tests.unit.transforms.helpers import make_time_record


def _record(value: float | None, hour: int, ticker: str):
    record = make_time_record(value, hour)
    record.ticker = ticker
    return record


def test_shift_time_moves_record_timestamp() -> None:
    record = make_time_record(10.0, 2)
    original_time = record.time
    transform = ShiftTimeRecordTransform(by="-1h")

    [shifted] = list(transform.apply(iter([record])))

    assert shifted.time == original_time - timedelta(hours=1)
    assert record.time == original_time
    assert shifted.value == 10.0


def test_stream_lag_copies_previous_partition_value() -> None:
    stream = iter(
        [
            _record(10.0, 0, "AAPL"),
            _record(11.0, 1, "AAPL"),
            _record(12.0, 2, "AAPL"),
            _record(20.0, 0, "MSFT"),
            _record(21.0, 1, "MSFT"),
        ]
    )
    transform = LagTransformer(
        field="value",
        to="value_lag_1",
        periods=1,
        partition_by="ticker",
    )

    out = list(transform.apply(stream))

    assert [getattr(record, "value_lag_1") for record in out] == [
        None,
        10.0,
        11.0,
        None,
        20.0,
    ]


def test_stream_lead_copies_future_partition_value() -> None:
    stream = iter(
        [
            _record(10.0, 0, "AAPL"),
            _record(11.0, 1, "AAPL"),
            _record(12.0, 2, "AAPL"),
            _record(20.0, 0, "MSFT"),
            _record(21.0, 1, "MSFT"),
        ]
    )
    transform = LeadTransformer(
        field="value",
        to="value_lead_1",
        periods=1,
        partition_by="ticker",
    )

    out = list(transform.apply(stream))

    assert [getattr(record, "value_lead_1") for record in out] == [
        11.0,
        12.0,
        None,
        21.0,
        None,
    ]


def test_stream_lead_binds_stream_partition() -> None:
    stream = iter(
        [
            _record(10.0, 0, "AAPL"),
            _record(11.0, 1, "AAPL"),
        ]
    )

    out = list(
        apply_transforms(
            stream,
            STREAM_TRANFORMS_EP,
            [
                TransformSpec(
                    name="lead",
                    params={"field": "value", "to": "value_lead_1", "periods": 1},
                )
            ],
            partition_by="ticker",
        )
    )

    assert [getattr(record, "value_lead_1") for record in out] == [11.0, None]


def test_stream_transform_partition_must_match_stream_partition() -> None:
    with pytest.raises(ValueError, match="must match the stream partition_by"):
        apply_transforms(
            iter([_record(10.0, 0, "AAPL")]),
            STREAM_TRANFORMS_EP,
            [
                TransformSpec(
                    name="lead",
                    params={
                        "field": "value",
                        "periods": 1,
                        "partition_by": "venue",
                    },
                )
            ],
            partition_by="ticker",
        )


@pytest.mark.parametrize("transform_cls", [LagTransformer, LeadTransformer])
def test_period_shift_requires_positive_periods(transform_cls) -> None:
    with pytest.raises(ValueError, match="periods must be a positive integer"):
        transform_cls(field="value", periods=0)
