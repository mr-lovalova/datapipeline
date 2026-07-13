from datetime import datetime, timezone

from datapipeline.utils.time import parse_cadence


_UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def floor_time_to_bucket(ts: datetime, bucket: str) -> datetime:
    """Floor on a continuous UTC grid, treating naive timestamps as UTC."""
    step = parse_cadence(bucket)
    if ts.tzinfo is None:
        epoch = _UTC_EPOCH.replace(tzinfo=None)
        return epoch + ((ts - epoch) // step) * step

    utc_timestamp = ts.astimezone(timezone.utc)
    floored = _UTC_EPOCH + ((utc_timestamp - _UTC_EPOCH) // step) * step
    return floored.astimezone(ts.tzinfo)
