from datetime import datetime, timedelta, timezone

from datapipeline.utils.time import parse_cadence


_UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def floor_time_to_bucket(ts: datetime, bucket: str) -> datetime:
    """Floor on a continuous UTC grid, treating naive timestamps as UTC."""
    return floor_time_to_cadence(ts, parse_cadence(bucket))


def floor_time_to_cadence(ts: datetime, cadence: timedelta) -> datetime:
    """Floor on a pre-parsed cadence without reparsing it for every record."""
    if ts.tzinfo is None:
        epoch = _UTC_EPOCH.replace(tzinfo=None)
        return epoch + ((ts - epoch) // cadence) * cadence

    utc_timestamp = ts.astimezone(timezone.utc)
    floored = _UTC_EPOCH + ((utc_timestamp - _UTC_EPOCH) // cadence) * cadence
    return floored.astimezone(ts.tzinfo)
