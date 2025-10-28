from collections.abc import Iterator
from typing import Any

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


def _normalize_op(op: str) -> str:
    op = (op or "").strip()
    return _ALIAS.get(op, op)


def filter(
    stream: Iterator[Any],
    *,
    operator: str,
    field: str,
    comparand: Any,
) -> Iterator[Any]:
    """Generic filter transform.

    Parameters
    - operator: one of eq, ne, lt, le, gt, ge, in, nin (case-sensitive), or a common alias
    - field: record attribute/key to compare
    - comparand: scalar for unary operators; list/tuple/set for membership (in/nin)
    """

    if is_missing(comparand):
        # Skip filter when comparand is an unresolved placeholder.
        return stream

    op = _normalize_op(operator)
    fn = None
    try:
        fn = load_ep(FILTERS_EP, op)
    except Exception:
        fn = getattr(_filters, op, None)
    if fn is None:
        raise ValueError(
            f"Unsupported filter operator: {operator!r} (normalized: {op!r})"
        )
    return fn(stream, field, comparand)
