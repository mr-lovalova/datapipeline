from datetime import datetime, timedelta

from datapipeline.config.dataset.normalize import floor_time_to_cadence
from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence


def _anchor_time(item: FeatureRecord | FeatureRecordSequence) -> datetime:
    if isinstance(item, FeatureRecord):
        return item.record.time
    if not item.records:
        raise ValueError(f"Feature sequence '{item.id}' has no records.")
    return item.records[-1].time


def group_key_for(
    item: FeatureRecord | FeatureRecordSequence,
    cadence: timedelta,
) -> tuple:
    return (floor_time_to_cadence(_anchor_time(item), cadence), *item.entity_key)
