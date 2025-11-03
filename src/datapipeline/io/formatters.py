from typing import Any
import json


class JsonLineFormatter:
    def __call__(self, rec: dict) -> str:
        return json.dumps(rec, ensure_ascii=False, default=str) + "\n"


class PrintLineFormatter:
    def __call__(self, rec: dict) -> str:
        return f"group={rec['key']}: {rec['values']}\n"


class CsvRowFormatter:
    def __call__(self, rec: dict) -> list:
        return [rec.get("key", ""), rec.get("values", "")]


class PickleFormatter:
    def __call__(self, rec: dict) -> Any:
        # stable tuple that your downstream expects
        return (rec["key"], rec["values"])
