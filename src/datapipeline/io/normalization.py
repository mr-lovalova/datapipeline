import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from typing import Any, Literal

View = Literal["flat", "raw", "values"]


@dataclass(frozen=True)
class NormalizedRow:
    key: Any
    kind: str
    fields: dict[str, Any]
    raw: Any


def normalized_record_row(item: Any) -> NormalizedRow:
    raw = _jsonable(item)
    fields: dict[str, Any] = {}
    if isinstance(raw, dict):
        for key, value in sorted(raw.items(), key=lambda kv: str(kv[0])):
            flatten_fields(str(key), value, fields)
    else:
        flatten_fields("value", raw, fields)
    return NormalizedRow(
        key=_record_key(item),
        kind=type(item).__name__,
        fields=fields,
        raw=raw,
    )


def normalized_row(
    *,
    key: Any,
    kind: str,
    raw: Any,
    fields: dict[str, Any],
) -> NormalizedRow:
    return NormalizedRow(
        key=key,
        kind=kind,
        fields=fields,
        raw=raw,
    )


def normalized_payload(row: NormalizedRow, view: View = "flat") -> dict[str, Any]:
    key = _normalize_key_struct(row.key)
    base = {"key": key, "kind": row.kind}
    if view == "flat":
        return {**base, "fields": row.fields}
    if view == "raw":
        return {**base, "raw": row.raw}
    if view == "values":
        return {**base, "values": _ordered_values(row.fields)}
    raise ValueError(f"Unsupported view '{view}'")


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


def _ordered_values(fields: dict[str, Any]) -> list[Any]:
    return [fields[key] for key in sorted(fields)]


def _normalize_key_struct(key: Any) -> Any:
    if isinstance(key, tuple):
        return list(key)
    return key


def _record_key(value: Any) -> Any:
    direct = getattr(value, "time", None)
    if direct is not None:
        return direct
    record = getattr(value, "record", None)
    if record is not None:
        return getattr(record, "time", None)
    return None


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
