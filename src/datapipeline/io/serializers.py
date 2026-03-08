import json
from datetime import date, datetime
from typing import Any

from datapipeline.io.normalization import (
    NormalizedRow,
    View,
    normalized_payload,
)


class JsonLineSerializer:
    def __init__(self, view: View) -> None:
        self._view = view

    def __call__(self, item: NormalizedRow) -> str:
        return json.dumps(
            normalized_payload(item, self._view),
            ensure_ascii=False,
            default=str,
        ) + "\n"


class CsvRowSerializer:
    def __init__(self, view: View) -> None:
        if view not in {"flat", "values"}:
            raise ValueError("csv output supports only view='flat' or view='values'")
        self._view = view

    def __call__(self, item: NormalizedRow) -> dict[str, Any]:
        out: dict[str, Any] = {}
        _add_key_columns(item.key, out)
        out["kind"] = item.kind
        if self._view == "flat":
            for field, value in item.fields.items():
                out[f"field.{field}"] = value
            return out

        payload = normalized_payload(item, "values")
        for idx, value in enumerate(payload["values"]):
            out[f"value_{idx}"] = value
        return out


class PickleSerializer:
    def __init__(self, view: View) -> None:
        self._view = view

    def __call__(self, item: NormalizedRow) -> Any:
        return normalized_payload(item, self._view)


class TextLineSerializer:
    def __call__(self, item: NormalizedRow) -> str:
        raw = item.raw
        if isinstance(raw, str):
            text = raw
        elif isinstance(raw, dict) and isinstance(raw.get("text"), str):
            text = raw["text"]
        else:
            text = json.dumps(raw, ensure_ascii=False, default=str)
        return text if text.endswith("\n") else f"{text}\n"


def json_line_serializer(
    view: View = "flat",
) -> JsonLineSerializer:
    return JsonLineSerializer(view)


def csv_row_serializer(
    view: View = "flat",
) -> CsvRowSerializer:
    return CsvRowSerializer(view)


def pickle_serializer(
    view: View = "flat",
) -> PickleSerializer:
    return PickleSerializer(view)


def text_line_serializer() -> TextLineSerializer:
    return TextLineSerializer()


def _add_key_columns(key: Any, row: dict[str, Any]) -> None:
    if isinstance(key, (tuple, list)):
        for idx, value in enumerate(key):
            row[f"key_{idx}"] = _csv_cell_value(value)
        return
    row["key"] = _csv_cell_value(key)


def _csv_cell_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return str(value)
    return value
