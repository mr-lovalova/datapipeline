from collections.abc import Iterator, Mapping
from typing import Any

from datapipeline.domain.record import TemporalRecord

from {{PACKAGE_NAME}}.domains.equity.model import EquityPairRecord


def _safe_ratio(num: float, den: float, default: float = 0.0) -> float:
    if den == 0:
        return default
    return num / den


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _metric_with_fallback(record: TemporalRecord, primary: str, fallback: str) -> float:
    value = getattr(record, primary, None)
    if value is None:
        value = getattr(record, fallback, None)
    return _as_float(value, default=0.0)


def compose_equity_pair_aapl_msft(
    inputs: Mapping[str, Iterator[TemporalRecord]],
    *,
    context: Any,
    driver: str | None = None,
    **params: Any,
) -> Iterator[EquityPairRecord]:
    """
    Compose ticker-specific streams into a single pair-analytics stream.

    Expected aliases in contract inputs:
    - aapl=equity.aapl
    - msft=equity.msft
    """
    del context
    del driver  # alignment is explicit and symmetric for this pair.
    del params

    aapl_iter = iter(inputs["aapl"])
    msft_iter = iter(inputs["msft"])

    aapl = next(aapl_iter, None)
    msft = next(msft_iter, None)
    prev_aapl_close: float | None = None
    prev_msft_close: float | None = None

    while aapl is not None and msft is not None:
        if aapl.time < msft.time:
            aapl = next(aapl_iter, None)
            continue
        if msft.time < aapl.time:
            msft = next(msft_iter, None)
            continue

        aapl_close = _as_float(getattr(aapl, "close", None), default=0.0)
        msft_close = _as_float(getattr(msft, "close", None), default=0.0)
        aapl_adv5 = _metric_with_fallback(aapl, "adv5", "dollar_volume")
        msft_adv5 = _metric_with_fallback(msft, "adv5", "dollar_volume")

        if prev_aapl_close is None or prev_msft_close is None:
            return_spread = 0.0
        else:
            aapl_ret = _safe_ratio(aapl_close - prev_aapl_close, prev_aapl_close)
            msft_ret = _safe_ratio(msft_close - prev_msft_close, prev_msft_close)
            return_spread = aapl_ret - msft_ret

        yield EquityPairRecord(
            time=aapl.time,
            close_ratio=_safe_ratio(aapl_close, msft_close),
            return_spread_1d=return_spread,
            adv5_ratio=_safe_ratio(aapl_adv5, msft_adv5),
        )

        prev_aapl_close = aapl_close
        prev_msft_close = msft_close
        aapl = next(aapl_iter, None)
        msft = next(msft_iter, None)
