from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime
from itertools import groupby
from typing import Any

from datapipeline.config.dataset.normalize import floor_time_to_bucket
from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector, vectorize_record_group
from datapipeline.pipelines.vector.keygen import group_key_for
from datapipeline.utils.time import parse_cadence, parse_datetime


def _close_iterator(iterator) -> None:
    closer = getattr(iterator, "close", None)
    if callable(closer):
        closer()


def vector_assemble_stage(
    merged: Iterator[FeatureRecord | FeatureRecordSequence],
    group_by_cadence: str,
) -> Iterator[tuple[tuple, Vector]]:
    try:
        for group_key, group in groupby(
            merged, key=lambda fr: group_key_for(fr, group_by_cadence)
        ):
            feature_map = defaultdict(list)
            for fr in group:
                if isinstance(fr, FeatureRecordSequence):
                    feature_map[fr.id].extend(fr.values)
                else:
                    feature_map[fr.id].append(fr.value)
            vector = vectorize_record_group(feature_map)
            yield group_key, vector
    finally:
        _close_iterator(merged)


def window_keys(
    start: datetime | None,
    end: datetime | None,
    cadence: str | None,
) -> Iterator[tuple] | None:
    if start is None or end is None or cadence is None:
        return None
    current = floor_time_to_bucket(start, cadence)
    stop = floor_time_to_bucket(end, cadence)
    step = parse_cadence(cadence)
    if stop < current:
        return None

    def _iter():
        t = current
        while t <= stop:
            yield (t,)
            t = t + step

    return _iter()


def sample_domain_window_keys(
    start: datetime | None,
    end: datetime | None,
    cadence: str,
    sample_keys: Sequence[str],
    domain: Sequence[Mapping[str, Any]],
) -> Iterator[tuple] | None:
    if start is None or end is None:
        return None
    if not sample_keys:
        return window_keys(start, end, cadence)
    global_start = floor_time_to_bucket(start, cadence)
    global_end = floor_time_to_bucket(end, cadence)
    step = parse_cadence(cadence)

    prepared = []
    for entry in domain:
        key_values = entry.get("key")
        if not isinstance(key_values, list):
            continue
        if len(key_values) != len(sample_keys):
            continue
        entry_start = _domain_time(entry.get("start"))
        entry_end = _domain_time(entry.get("end"))
        if entry_start is None or entry_end is None:
            continue
        domain_start = max(global_start, floor_time_to_bucket(entry_start, cadence))
        domain_end = min(global_end, floor_time_to_bucket(entry_end, cadence))
        if domain_start <= domain_end:
            prepared.append((tuple(key_values), domain_start, domain_end))
    prepared.sort(key=lambda item: item[0])

    def _iter():
        current = global_start
        while current <= global_end:
            for key_values, domain_start, domain_end in prepared:
                if domain_start <= current <= domain_end:
                    yield (current, *key_values)
            current = current + step

    return _iter()


def _domain_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value is None:
        return None
    try:
        return parse_datetime(str(value))
    except ValueError:
        return None


def align_stream(
    stream: Iterator[tuple[tuple, Vector]] | None,
    keys: Iterator[tuple] | None,
) -> Iterator[tuple[tuple, Vector]]:
    stream_iter = iter(stream or ())
    if keys is None:
        try:
            yield from stream_iter
        finally:
            _close_iterator(stream_iter)
        return
    keys_iter = iter(keys)
    try:
        current = next(stream_iter, None)
        for key in keys_iter:
            while current and current[0] < key:
                current = next(stream_iter, None)
            if current and current[0] == key:
                yield current
                current = next(stream_iter, None)
            else:
                yield (key, Vector(values={}))
    finally:
        _close_iterator(stream_iter)
        _close_iterator(keys_iter)


def sample_assemble_stage(
    feature_vectors: Iterator[tuple[tuple, Vector]],
    target_vectors: Iterator[tuple[tuple, Vector]] | None = None,
) -> Iterator[Sample]:
    feature_iter = iter(feature_vectors)
    target_iter = iter(target_vectors or ())

    def _advance(it):
        try:
            return next(it)
        except StopIteration:
            return None

    try:
        current_feature = _advance(feature_iter)
        current_target = _advance(target_iter)

        while current_feature:
            feature_key, feature_vector = current_feature
            targets = None

            while current_target and current_target[0] < feature_key:
                current_target = _advance(target_iter)

            if current_target and current_target[0] == feature_key:
                targets = current_target[1]
                current_target = _advance(target_iter)

            yield Sample(key=feature_key, features=feature_vector, targets=targets)
            current_feature = _advance(feature_iter)
    finally:
        _close_iterator(feature_iter)
        _close_iterator(target_iter)
