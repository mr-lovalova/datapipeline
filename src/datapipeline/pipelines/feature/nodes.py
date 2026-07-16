from collections import deque
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from datapipeline.artifacts.registry import SCALER_SPEC
from datapipeline.config.dataset.feature import FeatureRecordConfig, SequenceConfig
from datapipeline.domain.feature import FeatureRecord, FeatureSequence
from datapipeline.execution.context import PipelineContext
from datapipeline.pipelines.feature.projector import FeatureProjector
from datapipeline.pipelines.sort import SortProgress, batch_sort
from datapipeline.transforms.feature.scaler import FeatureScaler
from datapipeline.utils.time import floor_time_to_cadence, parse_cadence


def build_feature_stream(
    projector: FeatureProjector,
    config: FeatureRecordConfig,
    records: Iterable[Any],
) -> Iterator[FeatureRecord]:
    configs = (config,)
    for record in records:
        yield from projector.project(record, configs)


def scale_features(
    context: PipelineContext,
    features: Iterator[FeatureRecord],
) -> Iterator[FeatureRecord]:
    artifact = context.require_artifact(SCALER_SPEC)
    yield from FeatureScaler(artifact).apply(features)


def sequence_features(
    sequence: SequenceConfig,
    features: Iterator[FeatureRecord],
) -> Iterator[FeatureSequence]:
    sequencer = FeatureSequencer(sequence)
    for feature in features:
        result = sequencer.append(feature)
        if result is not None:
            yield result


class FeatureSequencer:
    def __init__(self, config: SequenceConfig) -> None:
        self.config = config
        self._active_key: tuple[str, tuple] | None = None
        self._window: deque[Any] = deque(maxlen=config.size)
        self._position = 0

    def append(self, feature: FeatureRecord) -> FeatureSequence | None:
        key = feature.id, feature.entity_key
        if key != self._active_key:
            self._active_key = key
            self._window.clear()
            self._position = 0

        position = self._position
        self._window.append(feature.value)
        self._position += 1

        window_start = position - self.config.size + 1
        if len(self._window) != self.config.size:
            return None
        if window_start % self.config.stride != 0:
            return None
        return FeatureSequence(
            time=feature.time,
            values=list(self._window),
            id=feature.id,
            entity_key=feature.entity_key,
        )


def order_feature_records(
    buffer_bytes: int,
    group_by_cadence: str | None,
    sample_keys: Sequence[str],
    progress: SortProgress,
    features: Iterator[FeatureRecord | FeatureSequence],
) -> Iterable[FeatureRecord | FeatureSequence]:
    key = _time_then_id
    if sample_keys and group_by_cadence is not None:
        key = _sample_group_then_time_and_id(group_by_cadence)
    return batch_sort(
        features,
        buffer_bytes=buffer_bytes,
        key=key,
        progress=progress,
    )


def _time_then_id(
    item: FeatureRecord | FeatureSequence,
) -> tuple[Any, str]:
    return item.time, item.id


def _sample_group_then_time_and_id(group_by_cadence: str):
    cadence = parse_cadence(group_by_cadence)

    def key(item: FeatureRecord | FeatureSequence) -> tuple[Any, ...]:
        time_value = item.time
        return (
            floor_time_to_cadence(time_value, cadence),
            *item.entity_key,
            time_value,
            item.id,
        )

    return key
