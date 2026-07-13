from datetime import datetime
from typing import Any

from datapipeline.config.dataset.normalize import floor_time_to_bucket


def _anchor_time(item: Any) -> datetime | None:
    rec = getattr(item, "record", None)
    if rec is not None:
        return getattr(rec, "time", None)
    recs = getattr(item, "records", None)
    return getattr(recs[-1], "time", None) if recs else None


def group_key_for(item: Any, cadence: str) -> tuple:
    t = _anchor_time(item)
    entity_key = getattr(item, "entity_key", ())
    return (floor_time_to_bucket(t, cadence), *entity_key)
