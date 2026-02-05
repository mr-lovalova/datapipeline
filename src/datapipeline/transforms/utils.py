import logging
import math
from dataclasses import is_dataclass, replace
from typing import Any


def is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def get_field(record: Any, field: str) -> Any:
    if isinstance(record, dict):
        return record.get(field)
    return getattr(record, field, None)


def clone_record(record: Any, **updates: Any) -> Any:
    """Return a shallow clone of record with updated fields."""
    if is_dataclass(record):
        return replace(record, **updates)
    if isinstance(record, dict):
        cloned = dict(record)
        cloned.update(updates)
        return cloned
    cloned = type(record)(**record.__dict__)
    for key, value in updates.items():
        setattr(cloned, key, value)
    return cloned


def clone_record_with_field(record: Any, field: str, value: Any) -> Any:
    """Return a shallow clone of record with a specific field updated."""
    return clone_record(record, **{field: value})


def clone_record_with_value(record: Any, value: Any) -> Any:
    """Return a shallow clone of *record* with its numeric value updated."""
    if not hasattr(record, "value") and not isinstance(record, dict):
        raise TypeError(
            f"clone_record_with_value expects an object with 'value'; got {type(record)!r}"
        )
    return clone_record_with_field(record, "value", value)


def floor_record_time(record: Any, cadence: str) -> Any:
    """Return a cloned record with time floored to cadence."""
    from datapipeline.config.dataset.normalize import floor_time_to_bucket

    return clone_record(record, time=floor_time_to_bucket(record.time, cadence))
