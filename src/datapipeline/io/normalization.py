import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from typing import Any, Literal

from datapipeline.domain.sample import Sample

ItemType = Literal["sample", "record"]
View = Literal["flat", "raw", "numeric"]


@dataclass(frozen=True)
class NormalizedRow:
    key: Any
    kind: str
    fields: dict[str, Any]
    raw: Any


def normalize_item(item: Any, item_type: ItemType) -> NormalizedRow:
    if item_type == "sample":
        return _normalize_sample(item)
    if item_type == "record":
        return _normalize_record(item)
    raise ValueError(f"Unsupported item_type '{item_type}'")


def normalized_payload(row: NormalizedRow, view: View = "flat") -> dict[str, Any]:
    key = _normalize_key_struct(row.key)
    base = {"key": key, "kind": row.kind}
    if view == "flat":
        return {**base, "fields": row.fields}
    if view == "raw":
        return {**base, "raw": row.raw}
    if view == "numeric":
        return {**base, "values": _numeric_values(row.fields)}
    raise ValueError(f"Unsupported view '{view}'")


def _numeric_values(fields: dict[str, Any]) -> list[float]:
    values: list[float] = []
    for key in sorted(fields):
        values.append(_coerce_numeric(key, fields[key]))
    return values


def _coerce_numeric(key: str, value: Any) -> float:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    raise ValueError(
        f"Field '{key}' is non-numeric in numeric view (got {type(value).__name__})"
    )


def _normalize_sample(sample: Sample) -> NormalizedRow:
    raw = sample.as_full_payload()
    fields: dict[str, Any] = {}
    _flatten_fields("features", sample.features.values, fields)
    if sample.targets is not None:
        _flatten_fields("targets", sample.targets.values, fields)
    return NormalizedRow(
        key=sample.key,
        kind=type(sample).__name__,
        fields=fields,
        raw=raw,
    )


def _normalize_record(item: Any) -> NormalizedRow:
    raw = _jsonable(item)
    fields: dict[str, Any] = {}
    if isinstance(raw, dict):
        for key, value in sorted(raw.items(), key=lambda kv: str(kv[0])):
            _flatten_fields(str(key), value, fields)
    else:
        _flatten_fields("value", raw, fields)
    return NormalizedRow(
        key=_record_key(item),
        kind=type(item).__name__,
        fields=fields,
        raw=raw,
    )


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


def _flatten_fields(prefix: str, value: Any, out: dict[str, Any]) -> None:
    if _is_scalar(value):
        out[prefix] = value
        return
    if isinstance(value, dict):
        for key, nested in sorted(value.items(), key=lambda kv: str(kv[0])):
            _flatten_fields(f"{prefix}.{key}", nested, out)
        return
    if isinstance(value, (list, tuple)):
        if all(_is_scalar(item) for item in value):
            for idx, nested in enumerate(value):
                out[f"{prefix}.{idx}"] = nested
            return
        out[prefix] = json.dumps(value, ensure_ascii=False, default=str)
        return
    out[prefix] = str(value)
