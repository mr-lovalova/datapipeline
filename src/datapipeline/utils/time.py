from datetime import timedelta, datetime, timezone


def parse_timecode(s: str) -> timedelta:
    """Parse duration strings like '30s', '10m', '1h', '2d' into timedelta."""
    unit = s[-1]
    value = int(s[:-1])
    if unit == 's':
        return timedelta(seconds=value)
    if unit == 'm':
        return timedelta(minutes=value)
    if unit == 'h':
        return timedelta(hours=value)
    if unit == 'd':
        return timedelta(days=value)
    raise ValueError(f"Unsupported time unit in: {s}")


def parse_datetime(s: str) -> datetime:
    """Parse an ISO-8601 datetime.

    - Accepts 'Z' or numeric offsets (e.g. '+00:00', '+01:30').
    - If input is timezone-naive, assume UTC.
    """
    if not isinstance(s, str):
        raise ValueError("parse_datetime expects a string")
    s2 = s[:-1] + "+00:00" if s.endswith("Z") else s
    try:
        dt = datetime.fromisoformat(s2)
    except Exception as e:
        raise ValueError(f"Invalid ISO-8601 datetime: {s}") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt
