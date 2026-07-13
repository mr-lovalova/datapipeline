import copy
import math
from typing import Any, TypeVar

from datapipeline.domain.record import TemporalRecord


TRecord = TypeVar("TRecord", bound=TemporalRecord)


def is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def finite_number(value: Any, field: str) -> float:
    if isinstance(value, (bool, str, bytes)):
        raise TypeError(f"Field {field!r} must contain numeric values")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"Field {field!r} must contain numeric values") from exc
    if not math.isfinite(number):
        raise ValueError(f"Field {field!r} must contain finite numeric values")
    return number


def get_field(record: object, field: str) -> Any:
    if not hasattr(record, field):
        raise KeyError(f"Record field '{field}' not found on {type(record).__name__}")
    return getattr(record, field)


def partition_key(
    record: object,
    partition_by: str | list[str] | tuple[str, ...] | None,
) -> tuple:
    if partition_by is None:
        return ()
    fields = [partition_by] if isinstance(partition_by, str) else partition_by
    values: list[Any] = []
    for field in fields:
        if not hasattr(record, field):
            raise KeyError(
                f"Partition field '{field}' not found on {type(record).__name__}"
            )
        value = getattr(record, field)
        if type(value) is float and not math.isfinite(value):
            raise ValueError(f"Partition field {field!r} must contain finite floats")
        values.append(value)
    return tuple(values)


def clone_record(record: TRecord, **updates: Any) -> TRecord:
    """Return a shallow clone of record with updated fields."""
    cloned = copy.copy(record)
    for key, value in updates.items():
        setattr(cloned, key, value)
    if "time" in updates:
        TemporalRecord.__post_init__(cloned)
    return cloned


def clone_record_with_field(record: TRecord, field: str, value: Any) -> TRecord:
    """Return a shallow clone of record with a specific field updated."""
    return clone_record(record, **{field: value})
