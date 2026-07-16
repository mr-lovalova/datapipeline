from datetime import timedelta

from datapipeline.domain.feature import FeatureRecord, FeatureSequence
from datapipeline.utils.time import floor_time_to_cadence


def group_key_for(
    item: FeatureRecord | FeatureSequence,
    cadence: timedelta,
) -> tuple:
    return (floor_time_to_cadence(item.time, cadence), *item.entity_key)
