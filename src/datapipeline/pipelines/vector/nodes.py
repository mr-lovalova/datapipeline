from collections.abc import Iterator
from itertools import groupby
from typing import Any

from datapipeline.domain.variable import VariableRecord, VariableSequence
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector


def _close_iterator(iterator) -> None:
    closer = getattr(iterator, "close", None)
    if callable(closer):
        closer()


def vector_assemble_stage(
    merged: Iterator[tuple[tuple, VariableRecord | VariableSequence]],
) -> Iterator[tuple[tuple, Vector]]:
    try:
        for group_key, group in groupby(merged, key=lambda item: item[0]):
            variable_map: dict[str, list[Any]] = {}
            sequence_ids: set[str] = set()
            for _, variable in group:
                is_sequence = isinstance(variable, VariableSequence)
                if variable.id in variable_map and is_sequence != (
                    variable.id in sequence_ids
                ):
                    raise ValueError(
                        f"Vector {variable.id!r} contains both scalar and sequence values."
                    )
                items = variable_map.setdefault(variable.id, [])
                if isinstance(variable, VariableSequence):
                    sequence_ids.add(variable.id)
                    items.extend(variable.values)
                else:
                    items.append(variable.value)
            values = {
                variable_id: (
                    items
                    if variable_id in sequence_ids or len(items) != 1
                    else items[0]
                )
                for variable_id, items in variable_map.items()
            }
            yield group_key, Vector(values=values)
    finally:
        _close_iterator(merged)


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
