from dataclasses import asdict
import json
from typing import Any


class JsonLineFormatter:
    def __call__(self, item: Any) -> str:
        payload = asdict(item)
        return json.dumps(asdict(payload), ensure_ascii=False, default=str) + "\n"


class PrintLineFormatter:
    def __call__(self, item: Any) -> str:
        return f"{item}\n"


class CsvRowFormatter:
    def __call__(self, item: Any) -> list[str | Any]:
        payload = asdict(item)
        key = payload.pop("key", "")
        if isinstance(key, tuple):
            key = list(key)
        if isinstance(key, (list, dict)):
            key_text = json.dumps(key, ensure_ascii=False, default=str)
        else:
            key_text = "" if key is None else str(key)
        payload_text = json.dumps(payload, ensure_ascii=False, default=str)
        return [key_text, payload_text]


class PickleFormatter:
    def __call__(self, item: Any) -> Any:
        return item
