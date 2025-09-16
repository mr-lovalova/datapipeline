from __future__ import annotations
from typing import Any, Callable, Iterator, Optional
from datetime import datetime, timezone
from datapipeline.utils.time import parse_datetime
import operator as _op


def get_field(obj: Any, field: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(field, default)
    return getattr(obj, field, default)


def coerce_datetime(value: Any) -> Optional[datetime]:
    """Return tz-aware UTC datetime if value is a datetime or ISO string; else None."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            return parse_datetime(value).astimezone(timezone.utc)
        except ValueError:
            return None
    return None


def compare_values(left: Any, right: Any, op: Callable[[Any, Any], bool]) -> bool:
    """
    Compare left and right using op, but if either side looks like a datetime,
    coerce BOTH to tz-aware UTC datetimes first. If only one side is datetime-like,
    comparison is false (types incompatible).
    """
    left_dt = coerce_datetime(left)
    right_dt = coerce_datetime(right)

    if (left_dt is not None) or (right_dt is not None):
        if left_dt is None or right_dt is None:
            return False
        return op(left_dt, right_dt)

    # Non-datetime comparison
    try:
        return op(left, right)
    except TypeError:
        return False


def make_binary_op(op: Callable[[Any, Any], bool]):
    """
    Build a record filter function with signature:
      fn(stream, *, x=<field path>, y=<literal>) -> filtered stream
    """
    def apply(stream: Iterator[Any], field: str, target: Any) -> Iterator[Any]:
        for record in stream:
            left = get_field(record, field)
            if compare_values(left, target, op):
                yield record
    return apply


# --- built-in filters ---
eq = make_binary_op(_op.eq)
ne = make_binary_op(_op.ne)
lt = make_binary_op(_op.lt)
le = make_binary_op(_op.le)
gt = make_binary_op(_op.gt)
ge = make_binary_op(_op.ge)


def _as_set(x: Any) -> set[Any]:
    if isinstance(x, (set, list, tuple)):
        return set(x)
    return {x}

# membership: value ∈ {targets}


def in_(stream: Iterator[Any], field: str, target: Any) -> Iterator[Any]:
    bag = _as_set(target)
    for record in stream:
        if get_field(record, field) in bag:
            yield record


def nin(stream: Iterator[Any], field: str, target: Any) -> Iterator[Any]:
    bag = _as_set(target)
    for record in stream:
        if get_field(record, field) not in bag:
            yield record
