from collections.abc import Iterator, Sequence
from datetime import datetime
from itertools import groupby
from typing import Any

from datapipeline.artifacts.models import SampleDomainEntry
from datapipeline.domain.feature import FeatureRecord, FeatureSequence
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.utils.time import floor_time_to_cadence, parse_cadence


def _close_iterator(iterator) -> None:
    closer = getattr(iterator, "close", None)
    if callable(closer):
        closer()


def vector_assemble_stage(
    merged: Iterator[tuple[tuple, FeatureRecord | FeatureSequence]],
) -> Iterator[tuple[tuple, Vector]]:
    try:
        for group_key, group in groupby(merged, key=lambda item: item[0]):
            feature_map: dict[str, list[Any]] = {}
            sequence_ids: set[str] = set()
            for _, fr in group:
                is_sequence = isinstance(fr, FeatureSequence)
                if fr.id in feature_map and is_sequence != (fr.id in sequence_ids):
                    raise ValueError(
                        f"Vector {fr.id!r} contains both scalar and sequence values."
                    )
                items = feature_map.setdefault(fr.id, [])
                if isinstance(fr, FeatureSequence):
                    sequence_ids.add(fr.id)
                    items.extend(fr.values)
                else:
                    items.append(fr.value)
            values = {
                feature_id: (
                    items if feature_id in sequence_ids or len(items) != 1 else items[0]
                )
                for feature_id, items in feature_map.items()
            }
            yield group_key, Vector(values=values)
    finally:
        _close_iterator(merged)


def window_keys(
    start: datetime | None,
    end: datetime | None,
    cadence: str | None,
) -> Iterator[tuple] | None:
    if start is None or end is None or cadence is None:
        return None
    step = parse_cadence(cadence)
    current = floor_time_to_cadence(start, step)
    stop = floor_time_to_cadence(end, step)
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
    domain: Sequence[SampleDomainEntry],
) -> Iterator[tuple] | None:
    if start is None or end is None:
        return None
    if not sample_keys:
        return window_keys(start, end, cadence)
    step = parse_cadence(cadence)
    global_start = floor_time_to_cadence(start, step)
    global_end = floor_time_to_cadence(end, step)

    prepared = []
    for entry in domain:
        if len(entry.key) != len(sample_keys):
            raise ValueError(
                "Vector metadata sample-domain key length does not match sample.keys."
            )
        domain_start = max(global_start, floor_time_to_cadence(entry.start, step))
        domain_end = min(global_end, floor_time_to_cadence(entry.end, step))
        if domain_start <= domain_end:
            prepared.append((tuple(entry.key), domain_start, domain_end))
    prepared.sort(key=lambda item: item[0])

    def _iter():
        current = global_start
        while current <= global_end:
            for key_values, domain_start, domain_end in prepared:
                if domain_start <= current <= domain_end:
                    yield (current, *key_values)
            current = current + step

    return _iter()


def align_stream(
    stream: Iterator[tuple[tuple, Vector]],
    keys: Iterator[tuple] | None,
) -> Iterator[tuple[tuple, Vector]]:
    stream_iter = iter(stream)
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
            while current is not None and current[0] < key:
                current = next(stream_iter, None)
            if current is not None and current[0] == key:
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
    target_iter = iter(()) if target_vectors is None else iter(target_vectors)

    try:
        current_feature = next(feature_iter, None)
        current_target = next(target_iter, None)

        while current_feature is not None:
            feature_key, feature_vector = current_feature
            targets = None

            while current_target is not None and current_target[0] < feature_key:
                current_target = next(target_iter, None)

            if current_target is not None and current_target[0] == feature_key:
                targets = current_target[1]
                current_target = next(target_iter, None)

            yield Sample(key=feature_key, features=feature_vector, targets=targets)
            current_feature = next(feature_iter, None)
    finally:
        _close_iterator(feature_iter)
        _close_iterator(target_iter)
