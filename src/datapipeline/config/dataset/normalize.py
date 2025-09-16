from datetime import datetime


def floor_time_to_resolution(ts: datetime, resolution: str) -> datetime:
    if resolution == "1h":
        return ts.replace(minute=0, second=0, microsecond=0)
    elif resolution == "10min":
        floored_minute = (ts.minute // 10) * 10
        return ts.replace(minute=floored_minute, second=0, microsecond=0)
    raise ValueError(f"Unsupported granularity: {resolution}")
