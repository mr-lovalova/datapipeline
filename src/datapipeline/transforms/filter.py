from collections.abc import Iterator
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


def normalize_operator(op: str) -> str:
    op = (op or "").strip()
    return _ALIAS.get(op, op)


def resolve_filter(
    operator: str,
    *,
    comparand: Any,
) -> tuple[str, Any | None]:
    """Resolve a normalized operator and callable filter function.

    Returns (op, fn) where fn may be None if comparand is missing.
    """
    if is_missing(comparand):
        return "", None

    op = normalize_operator(operator)
    fn = None
    try:
        fn = load_ep(FILTERS_EP, op)
    except Exception:
        fn = getattr(_filters, op, None)
    if fn is None:
        raise ValueError(
            f"Unsupported filter operator: {operator!r} (normalized: {op!r})"
        )
    return op, fn


def apply_filter(
    stream: Iterator[Any],
    *,
    field_getter: Callable[[Any, str], Any],
    operator: str,
    field: str,
    comparand: Any,
) -> Iterator[Any]:
    op, fn = resolve_filter(operator, comparand=comparand)
    if fn is None:
        return stream
    if getattr(fn, "__module__", None) != _filters.__name__:
        return fn(stream, field, comparand)

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
            f"Unsupported filter operator: {operator!r} (normalized: {op!r})"
        )

    def apply_cmp() -> Iterator[Any]:
        for record in stream:
            left = field_getter(record, field)
            if _filters.compare_values(left, comparand, cmp):
                yield record

    return apply_cmp()


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
    return apply_filter(
        stream,
        field_getter=_filters.get_field,
        operator=operator,
        field=field,
        comparand=comparand,
    )
