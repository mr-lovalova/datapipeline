from collections import defaultdict
from itertools import groupby
from typing import Any, Iterable, Iterator, Optional, Sequence, Tuple, Mapping

from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.domain.vector import Vector, vectorize_record_group
from datapipeline.pipeline.utils.memory_sort import memory_sorted
from datapipeline.pipeline.utils.transform_utils import apply_transforms
from datapipeline.plugins import FEATURE_TRANSFORMS_EP, VECTOR_TRANSFORMS_EP, RECORD_TRANSFORMS_EP, STREAM_TRANFORMS_EP, DEBUG_TRANSFORMS_EP

from datapipeline.config.dataset.normalize import floor_time_to_resolution
from datapipeline.domain.record import TemporalRecord
from datapipeline.pipeline.utils.keygen import RecordKeyGenerator
from datapipeline.registries.registries import record_operations, mappers, stream_sources, stream_operations, debug_operations
from datapipeline.sources.models.source import Source


def open_source_stream(stream_alias: str) -> Source:
    return stream_sources.get(stream_alias).stream()


def build_record_stream(record_stream: Iterable[Mapping[str, Any]], stream_id: str) -> Iterator[TemporalRecord]:
    """Map dto's to TemporalRecord instances."""
    mapper = mappers.get(stream_id)
    return mapper(record_stream)


def apply_record_operations(record_stream: Iterable[TemporalRecord], stream_id: str) -> Iterator[TemporalRecord]:
    """Apply record transforms defined in contract policies in order."""
    steps = record_operations.get(stream_id)
    records = apply_transforms(record_stream, RECORD_TRANSFORMS_EP, steps)
    return records


def build_feature_stream(
    record_stream: Iterable[TemporalRecord],
    base_feature_id: str,
    group_by: str,
    partition_by: Any | None = None,
) -> Iterator[FeatureRecord]:

    keygen = RecordKeyGenerator(partition_by)

    def group_key(rec: TemporalRecord) -> tuple:
        return (floor_time_to_resolution(rec.time, group_by),)

    for rec in record_stream:
        yield FeatureRecord(
            record=rec,
            id=keygen.generate(base_feature_id, rec),
            group_key=group_key(rec),
        )


def regularize_feature_stream(
    feature_stream: Iterable[FeatureRecord],
    stream_id: str,
    batch_size: int,
) -> Iterator[FeatureRecord]:
    """Apply feature transforms defined in contract policies in order."""
    sorted = memory_sorted(
        feature_stream,
        batch_size=batch_size,
        key=lambda fr: (fr.id, fr.record.time),
    )
    transformed = apply_transforms(
        sorted, STREAM_TRANFORMS_EP, stream_operations.get(stream_id)
    )
    # Apply optional debug-only transforms (e.g., lint/identity checks)
    transformed = apply_transforms(
        transformed, DEBUG_TRANSFORMS_EP, debug_operations.get(stream_id)
    )
    return transformed


def apply_feature_transforms(
    feature_stream: Iterable[FeatureRecord],
    scale: Mapping[str, Any] | None = None,
    sequence: Mapping[str, Any] | None = None,
) -> Iterator[FeatureRecord | FeatureRecordSequence]:
    """
    Expects input sorted by (feature_id, record.time).
    Returns FeatureRecord unless sequence is set, in which case it may emit FeatureRecordSequence.
    """

    clauses: list[Mapping[str, Any]] = []
    if scale:
        scale_args = {} if scale is True else dict(scale)
        clauses.append({"scale": scale_args})

    if sequence:
        clauses.append({"sequence": dict(sequence)})

    transformed = apply_transforms(
        feature_stream, FEATURE_TRANSFORMS_EP, clauses)
    return transformed
    # return memory_sorted(transformed, batch_size=batch_size, key=canonical_key)


def vector_assemble_stage(merged: Iterator[FeatureRecord | FeatureRecordSequence]) -> Iterator[Tuple[Any, Vector]]:
    """Group the merged feature stream by group_key.
    Coalesce each partitioned feature_id into record buckets.
    Yield (group_key, Vector) pairs ready for downstream consumption."""

    for group_key, group in groupby(merged, key=lambda fr: fr.group_key):
        feature_map = defaultdict(list)
        for fr in group:
            if isinstance(fr, FeatureRecordSequence):
                records = fr.records
            else:
                records = [fr.record]
            feature_map[fr.id].extend(records)
        yield group_key, vectorize_record_group(feature_map)


def vector_cleaning_stage(
    stream: Iterator[Tuple[Any, Vector]],
    clauses: Optional[Sequence[Mapping[str, Any]]],
) -> Iterator[Tuple[Any, Vector]]:
    """Apply configured vector transforms to the merged feature stream."""
    return apply_transforms(stream, VECTOR_TRANSFORMS_EP, clauses)
