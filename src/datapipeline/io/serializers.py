from collections.abc import Callable
from typing import Any

from datapipeline.io.normalization import (
    View,
    flat_payload,
    json_text,
    payload_for_view,
    raw_payload,
)


def json_line_serializer(view: View = "raw") -> Callable[[Any], str]:
    def serialize(item: Any) -> str:
        return json_text(payload_for_view(item, view)) + "\n"

    return serialize


def csv_row_serializer(view: View = "flat") -> Callable[[Any], dict[str, Any]]:
    if view != "flat":
        raise ValueError("csv output supports only view='flat'")
    return flat_payload


def pickle_serializer(view: View = "raw") -> Callable[[Any], Any]:
    if view != "raw":
        raise ValueError("pickle output supports only view='raw'")
    return raw_payload


def text_line_serializer() -> Callable[[Any], str]:
    def serialize(item: Any) -> str:
        raw = raw_payload(item)
        if isinstance(raw, str):
            text = raw
        elif isinstance(raw, dict) and isinstance(raw.get("text"), str):
            text = raw["text"]
        else:
            text = json_text(raw)
        return text if text.endswith("\n") else f"{text}\n"

    return serialize
