from collections.abc import Iterator
from itertools import groupby
from typing import Any

from datapipeline.domain.series import SeriesRecord, SeriesSequence
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector


def _close_iterator(iterator) -> None:
    closer = getattr(iterator, "close", None)
    if callable(closer):
        closer()


def assemble_vectors(
    merged: Iterator[tuple[tuple, SeriesRecord | SeriesSequence]],
) -> Iterator[tuple[tuple, Vector]]:
    try:
        for group_key, group in groupby(merged, key=lambda item: item[0]):
            values_by_series_id: dict[str, list[Any]] = {}
            sequence_ids: set[str] = set()
            for _, record in group:
                is_sequence = isinstance(record, SeriesSequence)
                if record.id in values_by_series_id and is_sequence != (
                    record.id in sequence_ids
                ):
                    raise ValueError(
                        f"Vector {record.id!r} contains both scalar and sequence values."
                    )
                items = values_by_series_id.setdefault(record.id, [])
                if isinstance(record, SeriesSequence):
                    sequence_ids.add(record.id)
                    items.extend(record.values)
                else:
                    items.append(record.value)
            values = {
                series_id: (
                    items if series_id in sequence_ids or len(items) != 1 else items[0]
                )
                for series_id, items in values_by_series_id.items()
            }
            yield group_key, Vector(values=values)
    finally:
        _close_iterator(merged)


def align_vectors_to_keys(
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


def assemble_samples(
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
