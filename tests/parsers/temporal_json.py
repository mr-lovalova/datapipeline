from datetime import datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from datapipeline.domain.record import TemporalRecord
from datapipeline.sources.models.parser import DataParser
from datapipeline.utils.time import parse_datetime


def _resolve_tz(name: str | None) -> timezone:
    if not name:
        return timezone.utc
    if name.upper() == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(name)
    except Exception:
        return timezone.utc


def _dig(container: Mapping[str, Any], path: str) -> Any:
    current: Any = container
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


class TemporalJsonPathParser(DataParser[TemporalRecord]):
    """Parse nested JSON dicts (e.g., json-lines) into TemporalRecords (test-only)."""

    def __init__(
        self,
        *,
        time_path: str,
        value_path: str,
        attributes: Mapping[str, str] | None = None,
        timezone_name: str | None = "UTC",
    ) -> None:
        self.time_path = time_path
        self.value_path = value_path
        self.attributes = dict(attributes or {})
        self.tz = _resolve_tz(timezone_name)

    def parse(self, raw: Mapping[str, Any]) -> TemporalRecord | None:
        time_raw = _dig(raw, self.time_path)
        value_raw = _dig(raw, self.value_path)
        if time_raw is None:
            return None

        ts = parse_datetime(str(time_raw))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=self.tz)
        else:
            ts = ts.astimezone(self.tz)

        value = _coerce_float(value_raw)
        rec = TemporalRecord(time=ts)
        setattr(rec, "value", value)
        for attr, path in self.attributes.items():
            setattr(rec, attr, _dig(raw, path))
        return rec
