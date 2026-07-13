from collections import deque
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from datapipeline.config.dataset.feature import SequenceConfig
from datapipeline.config.dataset.normalize import floor_time_to_cadence
from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.execution.context import PipelineContext
from datapipeline.pipelines.feature.keygen import FeatureIdGenerator
from datapipeline.pipelines.shared.sort import batch_sort
from datapipeline.services.artifacts import SCALER_SPEC
from datapipeline.transforms.feature.scaler import FeatureScaler
from datapipeline.transforms.utils import get_field, partition_key
from datapipeline.utils.time import parse_cadence


def build_feature_stream(
    feature_id: str,
    field: str,
    feature_id_by: tuple[str, ...] | None,
    sample_keys: Sequence[str],
    records: Iterable[Any],
) -> Iterator[FeatureRecord]:
    keygen = FeatureIdGenerator(feature_id_by)
    sample_key_contract = SampleKeyContract(sample_keys)
    for rec in records:
        if not _record_has_field(rec, field):
            raise KeyError(f"Record field '{field}' not found on {type(rec).__name__}")
        entity_key = partition_key(rec, tuple(sample_keys))
        sample_key_contract.validate(entity_key)
        yield FeatureRecord(
            record=rec,
            id=keygen.generate(feature_id, rec),
            value=get_field(rec, field),
            entity_key=entity_key,
        )


def scale_features(
    context: PipelineContext,
    features: Iterator[FeatureRecord],
) -> Iterator[FeatureRecord]:
    artifact = context.require_artifact(SCALER_SPEC)
    yield from FeatureScaler(artifact).apply(features)


def sequence_features(
    sequence: SequenceConfig,
    features: Iterator[FeatureRecord],
) -> Iterator[FeatureRecordSequence]:
    windows: dict[tuple[str, tuple], deque[FeatureRecord]] = {}
    positions: dict[tuple[str, tuple], int] = {}

    for feature in features:
        key = feature.id, feature.entity_key
        window = windows.setdefault(key, deque(maxlen=sequence.size))
        position = positions.get(key, 0)
        window.append(feature)
        window_start = position - sequence.size + 1
        if len(window) == sequence.size and window_start % sequence.stride == 0:
            yield FeatureRecordSequence(
                records=[item.record for item in window],
                values=[item.value for item in window],
                id=feature.id,
                entity_key=feature.entity_key,
            )
        positions[key] = position + 1


def order_feature_records(
    context: PipelineContext,
    group_by_cadence: str | None,
    sample_keys: Sequence[str],
    features: Iterator[FeatureRecord | FeatureRecordSequence],
) -> Iterable[FeatureRecord | FeatureRecordSequence]:
    key = _time_then_id
    if sample_keys and group_by_cadence is not None:
        key = _sample_group_then_time_and_id(group_by_cadence)
    return batch_sort(
        features,
        buffer_bytes=context.runtime.execution.sort_buffer_bytes,
        key=key,
    )


def _time_then_id(
    item: FeatureRecord | FeatureRecordSequence,
) -> tuple[Any, str]:
    return _record_time(item), item.id


def _sample_group_then_time_and_id(group_by_cadence: str):
    cadence = parse_cadence(group_by_cadence)

    def key(item: FeatureRecord | FeatureRecordSequence) -> tuple[Any, ...]:
        time_value = _record_time(item)
        return (
            floor_time_to_cadence(time_value, cadence),
            *item.entity_key,
            time_value,
            item.id,
        )

    return key


def _record_time(item: FeatureRecord | FeatureRecordSequence) -> Any:
    if isinstance(item, FeatureRecord):
        return item.record.time
    return item.records[-1].time


def _record_has_field(record: Any, field: str) -> bool:
    if isinstance(record, dict):
        return field in record
    return hasattr(record, field)
