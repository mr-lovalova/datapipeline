
from datapipeline.domain.vector import Vector
from datapipeline.domain.feature import FeatureRecord
from typing import Any, Iterator, Tuple
from itertools import groupby
from collections import defaultdict
from datapipeline.pipeline.utils.transform_utils import filter_record_stream, transform_record_stream
from datapipeline.pipeline.utils.transform_utils import transform_feature_stream, record_to_feature
from datapipeline.pipeline.utils.ordering import canonical_key
from datapipeline.pipeline.utils.memory_sort import memory_sorted
from datapipeline.domain.vector import vectorize_record_group


def record_stage(raw_stream, filters, transforms):
    s = filter_record_stream(raw_stream, filters)
    return transform_record_stream(s, transforms)


def feature_stage(record_stream, cfg, group_by):
    s = record_to_feature(record_stream, cfg, group_by)
    s = memory_sorted(s, batch_size=100000, key=lambda fr: (fr.feature_id, getattr(fr.record, 'time', 0)))
    s = transform_feature_stream(s, cfg)
    return memory_sorted(s, batch_size=100000, key=canonical_key)


def vector_stage(merged: Iterator[FeatureRecord]) -> Iterator[Tuple[Any, Vector]]:
    """
    Precondition: merged_features is sorted by FeatureRecord.group_key.
    Yields (group_key, Vector).
    """
    for group_key, group in groupby(merged, key=lambda fr: fr.group_key):
        feature_map = defaultdict(list)
        for fr in group:
            records = fr.record if isinstance(fr.record, list) else [fr.record]
            feature_map[fr.feature_id].extend(records)
        yield group_key, vectorize_record_group(feature_map)
