import json
from datetime import date, datetime
from typing import Any, Literal

from datapipeline.io.normalization import View, normalize_item, normalized_payload

ItemType = Literal["sample", "record"]


class JsonLineSerializer:
    def __init__(self, item_type: ItemType, view: View) -> None:
        self._item_type = item_type
        self._view = view

    def __call__(self, item: Any) -> str:
        row = normalize_item(item, self._item_type)
        return json.dumps(
            normalized_payload(row, self._view),
            ensure_ascii=False,
            default=str,
        ) + "\n"


class PrintSerializer:
    def __init__(self, item_type: ItemType, view: View) -> None:
        self._item_type = item_type
        self._view = view

    def __call__(self, item: Any) -> str:
        row = normalize_item(item, self._item_type)
        return f"{normalized_payload(row, self._view)}\n"


class CsvRowSerializer:
    def __init__(self, item_type: ItemType, view: View) -> None:
        if view not in {"flat", "values"}:
            raise ValueError("csv output supports only view='flat' or view='values'")
        self._item_type = item_type
        self._view = view

    def __call__(self, item: Any) -> dict[str, Any]:
        row = normalize_item(item, self._item_type)
        out: dict[str, Any] = {}
        _add_key_columns(row.key, out)
        out["kind"] = row.kind
        if self._view == "flat":
            for field, value in row.fields.items():
                out[f"field.{field}"] = value
            return out

        payload = normalized_payload(row, "values")
        for idx, value in enumerate(payload["values"]):
            out[f"value_{idx}"] = value
        return out


class PickleSerializer:
    def __init__(self, item_type: ItemType, view: View) -> None:
        self._item_type = item_type
        self._view = view

    def __call__(self, item: Any) -> Any:
        row = normalize_item(item, self._item_type)
        return normalized_payload(row, self._view)


def json_line_serializer(
    item_type: ItemType = "sample",
    view: View = "flat",
) -> JsonLineSerializer:
    return JsonLineSerializer(item_type, view)


def print_serializer(
    item_type: ItemType = "sample",
    view: View = "flat",
) -> PrintSerializer:
    return PrintSerializer(item_type, view)


def csv_row_serializer(
    item_type: ItemType = "sample",
    view: View = "flat",
) -> CsvRowSerializer:
    return CsvRowSerializer(item_type, view)


def pickle_serializer(
    item_type: ItemType = "sample",
    view: View = "flat",
) -> PickleSerializer:
    return PickleSerializer(item_type, view)


def _add_key_columns(key: Any, row: dict[str, Any]) -> None:
    if isinstance(key, tuple):
        for idx, value in enumerate(key):
            row[f"key_{idx}"] = _csv_cell_value(value)
        return
    if isinstance(key, list):
        for idx, value in enumerate(key):
            row[f"key_{idx}"] = _csv_cell_value(value)
        return
    row["key"] = _csv_cell_value(key)


def _csv_cell_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return str(value)
    return value
