import copy
import math
from dataclasses import is_dataclass
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


def partition_key(record: Any, partition_by: str | list[str] | None) -> tuple:
    if not partition_by:
        return ()
    if isinstance(partition_by, str):
        return (get_field(record, partition_by),)
    return tuple(get_field(record, field) for field in partition_by)


def clone_record(record: Any, **updates: Any) -> Any:
    """Return a shallow clone of record with updated fields."""
    if is_dataclass(record):
        cloned = copy.copy(record)
        for key, value in updates.items():
            setattr(cloned, key, value)
        post_init = getattr(cloned, "__post_init__", None)
        if callable(post_init) and "time" in updates:
            post_init()
        return cloned
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


def floor_record_time(record: Any, cadence: str) -> Any:
    """Return a cloned record with time floored to cadence."""
    from datapipeline.config.dataset.normalize import floor_time_to_bucket

    return clone_record(record, time=floor_time_to_bucket(record.time, cadence))
