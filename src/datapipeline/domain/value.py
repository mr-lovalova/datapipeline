import math
from collections.abc import Mapping
from typing import Any


def normalize_data_value(value: Any) -> Any:
    """Use None for missing floats and reject infinity in data values."""

    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if math.isnan(value):
            return None
        raise ValueError("Data values must not contain infinity.")
    if isinstance(value, list):
        normalized_list = [normalize_data_value(item) for item in value]
        return (
            normalized_list
            if any(a is not b for a, b in zip(value, normalized_list))
            else value
        )
    if isinstance(value, tuple):
        normalized_tuple = tuple(normalize_data_value(item) for item in value)
        return (
            normalized_tuple
            if any(a is not b for a, b in zip(value, normalized_tuple))
            else value
        )
    if isinstance(value, Mapping):
        normalized_mapping = {
            key: normalize_data_value(item) for key, item in value.items()
        }
        return (
            normalized_mapping
            if any(normalized_mapping[key] is not item for key, item in value.items())
            else value
        )
    return value
