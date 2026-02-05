from datetime import datetime, timezone
from typing import Any

from datapipeline.sources.models.parser import DataParser

from {{PACKAGE_NAME}}.dtos.sandbox_ohlcv_dto import SandboxOhlcvDTO


def _parse_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return None


class SandboxOhlcvDTOParser(DataParser[SandboxOhlcvDTO]):
    def parse(self, raw: Any) -> SandboxOhlcvDTO | None:
        """
        Convert one raw item (row/dict/tuple/record) into a SandboxOhlcvDTO.

        - Return a DTO instance to keep the item, or None to drop it.
        - Keep this logic thin and mirror your source data.
        """
        if not isinstance(raw, dict):
            return None
        parsed_time = _parse_time(raw.get("time"))
        if parsed_time is None:
            return None
        return SandboxOhlcvDTO(
            time=parsed_time,
            open=float(raw["open"]),
            high=float(raw["high"]),
            low=float(raw["low"]),
            close=float(raw["close"]),
            volume=float(raw["volume"]),
            symbol=str(raw["symbol"]),
        )
