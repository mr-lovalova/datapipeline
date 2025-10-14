from datetime import datetime
import re


def floor_time_to_resolution(ts: datetime, resolution: str) -> datetime:
    """Floor a timestamp to the nearest resolution bucket.

    Supports patterns like '10m', '10min', '1h', '2h'.
    Minutes may be specified as 'm' or 'min'.
    """
    m = re.fullmatch(r"^(\d+)(m|min|h)$", resolution)
    if not m:
        raise ValueError(f"Unsupported granularity: {resolution}")
    n = int(m.group(1))
    unit = m.group(2)
    if n <= 0:
        raise ValueError("resolution must be positive")

    if unit in ("m", "min"):
        floored_minute = (ts.minute // n) * n
        return ts.replace(minute=floored_minute, second=0, microsecond=0)
    else:  # 'h'
        floored_hour = (ts.hour // n) * n
        return ts.replace(hour=floored_hour, minute=0, second=0, microsecond=0)
