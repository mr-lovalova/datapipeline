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


def clone_record_with_value(record: Any, value: Any) -> Any:
    """Return a shallow clone of *record* with its numeric value updated."""

    if hasattr(record, "value"):
        if is_dataclass(record):
            return replace(record, value=value)

        cloned = type(record)(**record.__dict__)
        cloned.value = value
        return cloned

    raise TypeError(f"clone_record_with_value expects an object with 'value'; got {type(record)!r}")
