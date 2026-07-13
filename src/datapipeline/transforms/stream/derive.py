from collections.abc import Iterator
from typing import Any, Literal

from datapipeline.domain.record import TemporalRecord
from datapipeline.transforms.interfaces import StreamTransformBase
from datapipeline.transforms.utils import clone_record_with_field, get_field, is_missing

Operator = Literal["add", "sub", "mul", "div"]
_MISSING = object()


class DeriveTransform(StreamTransformBase):
    """Derive one record field from a binary arithmetic operation."""

    def __init__(
        self,
        *,
        left: str,
        operator: Operator,
        to: str,
        right_field: str | None = None,
        right_value: Any = _MISSING,
    ) -> None:
        if not left:
            raise ValueError("left is required")
        if not to:
            raise ValueError("to is required")
        if operator not in {"add", "sub", "mul", "div"}:
            raise ValueError("operator must be one of: add, sub, mul, div")
        has_right_field = right_field is not None
        has_right_value = right_value is not _MISSING
        if has_right_field == has_right_value:
            raise ValueError("set exactly one of right_field or right_value")
        if right_field == "":
            raise ValueError("right_field must not be empty")

        self.left = left
        self.operator = operator
        self.to = to
        self.right_field = right_field
        self.right_value = right_value

    def apply(self, stream: Iterator[TemporalRecord]) -> Iterator[TemporalRecord]:
        for record in stream:
            left = self._number(get_field(record, self.left))
            right_source = (
                get_field(record, self.right_field)
                if self.right_field is not None
                else self.right_value
            )
            right = self._number(right_source)
            value = self._derive(left, right)
            yield clone_record_with_field(record, self.to, value)

    @staticmethod
    def _number(value: Any) -> float | None:
        if is_missing(value):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _derive(self, left: float | None, right: float | None) -> float | None:
        if left is None or right is None:
            return None
        if self.operator == "add":
            return left + right
        if self.operator == "sub":
            return left - right
        if self.operator == "mul":
            return left * right
        if right == 0:
            return None
        return left / right
