import json
from dataclasses import fields, is_dataclass
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
    flattened: dict[str, Any] = {}
    if isinstance(raw, dict):
        for key, value in sorted(raw.items(), key=lambda kv: str(kv[0])):
            flatten_fields(str(key), value, flattened)
        return flattened

    flatten_fields("value", raw, flattened)
    return flattened


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
    if not isinstance(value, type) and is_dataclass(value):
        attributes = getattr(value, "__dict__", None)
        if attributes is not None:
            return {
                name: _jsonable(field_value)
                for name, field_value in attributes.items()
                if not name.startswith("_")
            }
        return {
            field.name: _jsonable(getattr(value, field.name))
            for field in fields(value)
            if not field.name.startswith("_")
        }
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
