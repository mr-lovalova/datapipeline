from dataclasses import dataclass

from datapipeline.domain.record import TemporalRecord


@dataclass
class EquityRecord(TemporalRecord):
    """
    Domain record for 'equity'.
    """
    open: float
    high: float
    low: float
    close: float
    volume: float
    dollar_volume: float
    hl_range: float
    ticker: str  # equity ticker symbol
