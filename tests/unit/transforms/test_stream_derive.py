import pytest

from datapipeline.plugins import STREAM_TRANFORMS_EP
from datapipeline.transforms.engine import apply_transforms
from datapipeline.transforms.spec import TransformSpec
from datapipeline.transforms.stream.derive import DeriveTransform
from tests.unit.transforms.helpers import make_time_record


def test_derive_divides_two_fields() -> None:
    record = make_time_record(0.0, 0)
    record.close_lag_21 = 110.0
    record.close_lag_189 = 100.0
    transform = DeriveTransform(
        left="close_lag_21",
        operator="div",
        right_field="close_lag_189",
        to="close_ratio_21_189",
    )

    [out] = list(transform.apply(iter([record])))

    assert out.close_ratio_21_189 == 1.1
    assert not hasattr(record, "close_ratio_21_189")


def test_derive_accepts_right_value() -> None:
    record = make_time_record(0.0, 0)
    record.close_ratio_21_189 = 1.1
    transform = DeriveTransform(
        left="close_ratio_21_189",
        operator="sub",
        right_value=1.0,
        to="momentum_189_21",
    )

    [out] = list(transform.apply(iter([record])))

    assert out.momentum_189_21 == pytest.approx(0.1)


@pytest.mark.parametrize(
    ("operator", "expected"),
    [
        ("add", 8.0),
        ("sub", 2.0),
        ("mul", 15.0),
        ("div", 5.0 / 3.0),
    ],
)
def test_derive_supports_basic_operators(operator: str, expected: float) -> None:
    record = make_time_record(0.0, 0)
    record.left = 5.0
    record.right = 3.0
    transform = DeriveTransform(
        left="left",
        operator=operator,
        right_field="right",
        to="derived",
    )

    [out] = list(transform.apply(iter([record])))

    assert out.derived == expected


@pytest.mark.parametrize("right", [None, float("nan"), "not-a-number"])
def test_derive_writes_none_for_missing_or_non_numeric_inputs(right) -> None:
    record = make_time_record(0.0, 0)
    record.left = 5.0
    record.right = right
    transform = DeriveTransform(
        left="left",
        operator="add",
        right_field="right",
        to="derived",
    )

    [out] = list(transform.apply(iter([record])))

    assert out.derived is None


def test_derive_writes_none_for_divide_by_zero() -> None:
    record = make_time_record(0.0, 0)
    record.left = 5.0
    record.right = 0.0
    transform = DeriveTransform(
        left="left",
        operator="div",
        right_field="right",
        to="derived",
    )

    [out] = list(transform.apply(iter([record])))

    assert out.derived is None


def test_derive_entry_point_runs_from_config() -> None:
    record = make_time_record(0.0, 0)
    record.close_lag_21 = 110.0
    record.close_lag_189 = 100.0

    [out] = list(
        apply_transforms(
            iter([record]),
            STREAM_TRANFORMS_EP,
            [
                TransformSpec(
                    name="derive",
                    params={
                        "left": "close_lag_21",
                        "operator": "div",
                        "right_field": "close_lag_189",
                        "to": "close_ratio_21_189",
                    },
                ),
                TransformSpec(
                    name="derive",
                    params={
                        "left": "close_ratio_21_189",
                        "operator": "sub",
                        "right_value": 1.0,
                        "to": "momentum_189_21",
                    },
                ),
            ],
        )
    )

    assert out.close_ratio_21_189 == 1.1
    assert out.momentum_189_21 == pytest.approx(0.1)


def test_derive_requires_exactly_one_right_operand() -> None:
    message = "set exactly one of right_field or right_value"

    with pytest.raises(ValueError, match=message):
        DeriveTransform(left="left", operator="add", to="derived")

    with pytest.raises(ValueError, match=message):
        DeriveTransform(
            left="left",
            operator="add",
            right_field="right",
            right_value=1.0,
            to="derived",
        )


def test_derive_rejects_unknown_operator() -> None:
    with pytest.raises(ValueError, match="operator must be one of"):
        DeriveTransform(
            left="left",
            operator="pow",
            right_value=2.0,
            to="derived",
        )
