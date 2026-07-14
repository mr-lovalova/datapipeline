import json
from typing import Any

from datapipeline.io.normalization import (
    View,
    flat_payload,
    payload_for_view,
    raw_payload,
)


class JsonLineSerializer:
    def __init__(self, view: View) -> None:
        self._view = view

    def __call__(self, item: Any) -> str:
        return (
            json.dumps(
                payload_for_view(item, self._view),
                ensure_ascii=False,
                default=str,
            )
            + "\n"
        )


class CsvRowSerializer:
    def __init__(self, view: View) -> None:
        if view != "flat":
            raise ValueError("csv output supports only view='flat'")

    def __call__(self, item: Any) -> dict[str, Any]:
        return flat_payload(item)


class PickleSerializer:
    def __init__(self, view: View) -> None:
        if view != "raw":
            raise ValueError("pickle output supports only view='raw'")
        self._view = view

    def __call__(self, item: Any) -> Any:
        return raw_payload(item)


class TextLineSerializer:
    def __call__(self, item: Any) -> str:
        raw = raw_payload(item)
        if isinstance(raw, str):
            text = raw
        elif isinstance(raw, dict) and isinstance(raw.get("text"), str):
            text = raw["text"]
        else:
            text = json.dumps(raw, ensure_ascii=False, default=str)
        return text if text.endswith("\n") else f"{text}\n"


def json_line_serializer(
    view: View = "raw",
) -> JsonLineSerializer:
    return JsonLineSerializer(view)


def csv_row_serializer(
    view: View = "flat",
) -> CsvRowSerializer:
    return CsvRowSerializer(view)


def pickle_serializer(
    view: View = "raw",
) -> PickleSerializer:
    return PickleSerializer(view)


def text_line_serializer() -> TextLineSerializer:
    return TextLineSerializer()
