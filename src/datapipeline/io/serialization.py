from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


def _to_plain(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return {k: _to_plain(v) for k, v in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(k): _to_plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset, Sequence)) and not isinstance(value, (str, bytes, bytearray)):
        return [_to_plain(v) for v in value]
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
        return [_to_plain(v) for v in list(value)]
    if hasattr(value, "__dict__"):
        return {k: _to_plain(v) for k, v in vars(value).items() if not k.startswith("_")}
    return repr(value)


def normalize_output_record(
    key: Any,
    payload: Any,
    *,
    stage: int | None,
) -> dict:
    """Return a serialization-friendly mapping for the current stage."""
    if stage is None or stage >= 6:
        normalized_key = list(key) if isinstance(key, tuple) else key
        return {
            "key": _to_plain(normalized_key),
            "values": _to_plain(getattr(payload, "values", payload)),
        }

    feature_id = _to_plain(key)
    payload_plain = _to_plain(payload)
    return {
        "key": f"{feature_id}@stage{stage}",
        "values": payload_plain,
        "feature_id": feature_id,
        "stage": stage,
        "payload": payload_plain,
    }
