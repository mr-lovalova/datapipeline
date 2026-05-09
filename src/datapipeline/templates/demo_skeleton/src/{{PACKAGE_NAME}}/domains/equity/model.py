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


@dataclass
class EquityPairRecord(TemporalRecord):
    """
    Domain record for pair analytics.
    """
    close_ratio: float
    return_spread_1d: float
    adv5_ratio: float
