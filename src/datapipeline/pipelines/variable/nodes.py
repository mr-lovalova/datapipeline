from collections import deque
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from datapipeline.config.dataset.variable import SequenceConfig, VariableConfig
from datapipeline.domain.variable import VariableRecord, VariableSequence
from datapipeline.pipelines.variable.projector import VariableProjector
from datapipeline.pipelines.sort import SortProgress, batch_sort
from datapipeline.utils.time import floor_time_to_cadence, parse_cadence


def project_variable_records(
    projector: VariableProjector,
    config: VariableConfig,
    records: Iterable[Any],
) -> Iterator[VariableRecord]:
    for record in records:
        yield from projector.project(record, (config,))


def sequence_variables(
    sequence: SequenceConfig,
    variables: Iterator[VariableRecord],
) -> Iterator[VariableSequence]:
    sequencer = VariableSequencer(sequence)
    for variable in variables:
        result = sequencer.append(variable)
        if result is not None:
            yield result


class VariableSequencer:
    def __init__(self, config: SequenceConfig) -> None:
        self.config = config
        self._active_key: tuple[str, tuple] | None = None
        self._window: deque[Any] = deque(maxlen=config.size)
        self._position = 0

    def append(self, variable: VariableRecord) -> VariableSequence | None:
        key = variable.id, variable.entity_key
        if key != self._active_key:
            self._active_key = key
            self._window.clear()
            self._position = 0

        position = self._position
        self._window.append(variable.value)
        self._position += 1

        window_start = position - self.config.size + 1
        if len(self._window) != self.config.size:
            return None
        if window_start % self.config.stride != 0:
            return None
        return VariableSequence(
            time=variable.time,
            values=list(self._window),
            id=variable.id,
            entity_key=variable.entity_key,
        )


def order_variable_records(
    buffer_bytes: int,
    group_by_cadence: str | None,
    sample_keys: Sequence[str],
    progress: SortProgress,
    variables: Iterator[VariableRecord | VariableSequence],
) -> Iterable[VariableRecord | VariableSequence]:
    key = _time_then_id
    if sample_keys and group_by_cadence is not None:
        key = _sample_group_then_time_and_id(group_by_cadence)
    return batch_sort(
        variables,
        buffer_bytes=buffer_bytes,
        key=key,
        progress=progress,
    )


def _time_then_id(
    item: VariableRecord | VariableSequence,
) -> tuple[Any, str]:
    return item.time, item.id


def _sample_group_then_time_and_id(group_by_cadence: str):
    cadence = parse_cadence(group_by_cadence)

    def key(item: VariableRecord | VariableSequence) -> tuple[Any, ...]:
        time_value = item.time
        return (
            floor_time_to_cadence(time_value, cadence),
            *item.entity_key,
            time_value,
            item.id,
        )

    return key
