import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any, Literal

from datapipeline.domain.sample import Sample

View = Literal["flat", "raw"]


def payload_for_view(item: Any, view: View = "raw") -> Any:
    if view == "raw":
        return raw_payload(item)
    if view == "flat":
        return flat_payload(item)
    raise ValueError(f"Unsupported view '{view}'")


def raw_payload(item: Any) -> Any:
    if isinstance(item, Sample):
        return item.as_full_payload()
    return _jsonable(item)


def flat_payload(item: Any) -> dict[str, Any]:
    if isinstance(item, Sample):
        payload: dict[str, Any] = {}
        flatten_fields("key", _normalize_key_struct(item.key), payload)
        flatten_fields("features", item.features.values, payload)
        if item.targets is not None:
            flatten_fields("targets", item.targets.values, payload)
        return payload

    raw = raw_payload(item)
    payload: dict[str, Any] = {}
    if isinstance(raw, dict):
        for key, value in sorted(raw.items(), key=lambda kv: str(kv[0])):
            flatten_fields(str(key), value, payload)
        return payload

    flatten_fields("value", raw, payload)
    return payload


def flatten_fields(prefix: str, value: Any, out: dict[str, Any]) -> None:
    if _is_scalar(value):
        out[prefix] = value
        return
    if isinstance(value, dict):
        for key, nested in sorted(value.items(), key=lambda kv: str(kv[0])):
            flatten_fields(f"{prefix}.{key}", nested, out)
        return
    if isinstance(value, (list, tuple)):
        if all(_is_scalar(item) for item in value):
            for idx, nested in enumerate(value):
                out[f"{prefix}.{idx}"] = nested
            return
        out[prefix] = json.dumps(value, ensure_ascii=False, default=str)
        return
    out[prefix] = str(value)


def _normalize_key_struct(key: Any) -> Any:
    if isinstance(key, tuple):
        return list(key)
    return key


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if is_dataclass(value):
        attrs = getattr(value, "__dict__", None)
        if attrs is not None:
            return {k: _jsonable(v) for k, v in attrs.items() if not k.startswith("_")}
        return asdict(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    attrs = getattr(value, "__dict__", None)
    if attrs:
        return {k: _jsonable(v) for k, v in attrs.items() if not k.startswith("_")}
    return value


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool, datetime, date))
