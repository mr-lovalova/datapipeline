from typing import Any, Mapping, MutableMapping

PARTITION_SEP = "__"


def base_id(feature_id: str) -> str:
    return feature_id.split(PARTITION_SEP, 1)[0] if PARTITION_SEP in feature_id else feature_id


def partition_suffix(feature_id: str) -> str:
    if PARTITION_SEP in feature_id:
        return feature_id.split(PARTITION_SEP, 1)[1]
    return ""


def is_partitioned(feature_id: str) -> bool:
    return PARTITION_SEP in feature_id


def make_partition_id(base: str, suffix: str) -> str:
    return f"{base}{PARTITION_SEP}{suffix}" if suffix else base


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        # NaN check without numpy
        return value != value
    return False


def clone(values: Mapping[str, Any]) -> MutableMapping[str, Any]:
    if isinstance(values, MutableMapping):
        return type(values)(values)
    return dict(values)
