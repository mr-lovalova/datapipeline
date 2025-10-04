from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Literal, Sequence


def base_id(feature_id: str) -> str:
    return feature_id.split("__", 1)[0] if "__" in feature_id else feature_id


def normalize_key(feature_id: str, match_partition: Literal["base", "full"]) -> str:
    return feature_id if match_partition == "full" else base_id(feature_id)


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        # NaN check without numpy
        return value != value
    return False


def _reduce_list(values: list[Any], policy: Literal["last", "mean", "median"]) -> float | None:
    if not values:
        return None
    try:
        if policy == "last":
            return float(values[-1])
        if policy == "mean":
            return float(mean(float(v) for v in values))
        if policy == "median":
            return float(median(float(v) for v in values))
    except (TypeError, ValueError):
        return None
    return None


def to_numeric(
    value: Any,
    *,
    reduce: Literal["none", "last", "mean", "median"] = "none",
) -> float | None:
    if is_missing(value):
        return None
    if isinstance(value, list):
        if reduce == "none":
            return None
        return _reduce_list(value, policy=reduce)  # type: ignore[arg-type]
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@lru_cache(maxsize=128)
def _load_manifest(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise TypeError("Partition manifest must be a mapping")
    return data


def resolve_expected(
    manifest: str | None,
    expected: Sequence[str] | None,
    *,
    match_partition: Literal["base", "full"] = "base",
) -> list[str]:
    if expected is not None:
        return [str(x) for x in expected]
    if manifest:
        payload = _load_manifest(Path(manifest).expanduser().resolve())
        key = "partitions" if match_partition == "full" else "features"
        raw = payload.get(key)
        if not isinstance(raw, Sequence):
            raise TypeError(f"Partition manifest must contain a '{key}' list")
        return [str(x) for x in raw]
    return []

