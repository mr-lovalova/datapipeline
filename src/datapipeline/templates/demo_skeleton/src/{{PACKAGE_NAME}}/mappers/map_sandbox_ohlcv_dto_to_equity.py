from typing import Any, Iterator

from {{PACKAGE_NAME}}.domains.equity.model import EquityRecord
from {{PACKAGE_NAME}}.dtos.sandbox_ohlcv_dto import SandboxOhlcvDTO


def map_sandbox_ohlcv_dto_to_equity(
    stream: Iterator[SandboxOhlcvDTO],
    **params: Any,
) -> Iterator[EquityRecord]:
    """Map SandboxOhlcvDTO records to domain-level EquityRecord records."""
    for record in stream:
        yield EquityRecord(
            time=record.time,  # necessary for correct grouping and ordering
            value=record.close,  # assuming 'close' price is the main value

            # filterable fields
            open=record.open,
            high=record.high,
            low=record.low,
            close=record.close,
            volume=record.volume,
            dollar_volume=record.close * record.volume,
            hl_range=record.high - record.low,
            adv5=None,  # derived via stream rolling example
            ticker=record.symbol,
            # filterable fields
        )
