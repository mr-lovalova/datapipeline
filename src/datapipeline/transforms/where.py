from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, Callable

from datapipeline.filters import filters as _filters
from datapipeline.plugins import FILTERS_EP
from datapipeline.utils.load import load_ep
from datapipeline.utils.placeholders import is_missing


_ALIAS = {
    "equals": "eq",
    "equal": "eq",
    "==": "eq",
    "!=": "ne",
    ">": "gt",
    ">=": "ge",
    "<": "lt",
    "<=": "le",
    # Membership operators
    "in": "in_",
    "not in": "nin",
    "nin": "nin",
}
_ORDERING_OPS = {"eq", "ne", "lt", "le", "gt", "ge"}


def normalize_operator(op: str) -> str:
    op = (op or "").strip()
    return _ALIAS.get(op, op)


def resolve_where(
    operator: str,
    *,
    comparand: Any,
) -> tuple[str, Any]:
    """Resolve a normalized operator and callable predicate function."""

    op = normalize_operator(operator)
    fn = None
    try:
        fn = load_ep(FILTERS_EP, op)
    except Exception:
        fn = getattr(_filters, op, None)
    if fn is None:
        raise ValueError(
            f"Unsupported where operator: {operator!r} (normalized: {op!r})"
        )
    return op, fn


def apply_where(
    stream: Iterator[Any],
    *,
    field_getter: Callable[[Any, str], Any],
    operator: str,
    field: str,
    comparand: Any,
) -> Iterator[Any]:
    if is_missing(comparand):
        raise ValueError(
            f"Where comparand for field {field!r} operator {operator!r} "
            "is missing. Set the referenced value or remove the where clause."
        )
    op, fn = resolve_where(operator, comparand=comparand)
    if getattr(fn, "__module__", None) != _filters.__name__:
        return fn(stream, field, comparand)

    if field == "time" and op in _ORDERING_OPS:
        return _apply_time_where(stream, field_getter, op, comparand)

    if op in {"in_", "nin"}:
        bag = _filters._as_set(comparand)

        def apply_in() -> Iterator[Any]:
            for record in stream:
                left = field_getter(record, field)
                if (left in bag) == (op == "in_"):
                    yield record

        return apply_in()

    cmp = getattr(_filters._op, op, None)
    if cmp is None:
        raise ValueError(
            f"Unsupported where operator: {operator!r} (normalized: {op!r})"
        )

    def apply_cmp() -> Iterator[Any]:
        for record in stream:
            left = field_getter(record, field)
            if _filters.compare_values(left, comparand, cmp):
                yield record

    return apply_cmp()


def _apply_time_where(
    stream: Iterator[Any],
    field_getter: Callable[[Any, str], Any],
    op: str,
    comparand: Any,
) -> Iterator[Any]:
    target = _filters.coerce_datetime(comparand)
    if target is None:
        raise ValueError(
            f"Where comparand for field 'time' operator {op!r} "
            f"must be a valid datetime, got {comparand!r}."
        )
    cmp = getattr(_filters._op, op)

    def apply_time() -> Iterator[Any]:
        for record in stream:
            left = field_getter(record, "time")
            if not isinstance(left, datetime):
                raise TypeError(
                    "Where field 'time' must contain a datetime value, "
                    f"got {type(left).__name__}."
                )
            timestamp = (
                left.astimezone(timezone.utc)
                if left.tzinfo
                else left.replace(tzinfo=timezone.utc)
            )
            if cmp(timestamp, target):
                yield record

    return apply_time()


def where(
    stream: Iterator[Any],
    *,
    operator: str,
    field: str,
    comparand: Any,
) -> Iterator[Any]:
    """Keep records where a field comparison is true.

    Parameters
    - operator: one of eq, ne, lt, le, gt, ge, in, nin (case-sensitive), or a common alias
    - field: record attribute/key to compare
    - comparand: scalar for unary operators; list/tuple/set for membership (in/nin)
    """
    return apply_where(
        stream,
        field_getter=_filters.get_field,
        operator=operator,
        field=field,
        comparand=comparand,
    )
