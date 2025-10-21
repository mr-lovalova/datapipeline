from collections import defaultdict
from itertools import groupby
from typing import Any, Iterable, Iterator, Tuple, Mapping
from datapipeline.runtime import Runtime
from datapipeline.services.artifacts import load_artifact

from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.domain.vector import Vector, vectorize_record_group
from datapipeline.pipeline.utils.memory_sort import batch_sort
from datapipeline.pipeline.utils.transform_utils import apply_transforms
from datapipeline.plugins import FEATURE_TRANSFORMS_EP, VECTOR_TRANSFORMS_EP, RECORD_TRANSFORMS_EP, STREAM_TRANFORMS_EP, DEBUG_TRANSFORMS_EP

from datapipeline.domain.record import TemporalRecord
from datapipeline.pipeline.utils.keygen import FeatureIdGenerator, group_key_for
from datapipeline.services.constants import POSTPROCESS_TRANSFORMS, PARTIONED_IDS
from datapipeline.pipeline.postprocess_context import (
    set_expected_ids,
    reset_expected_ids,
)
from datapipeline.sources.models.source import Source
from datapipeline.pipeline.split import apply_split_stage as split_stage


def open_source_stream(runtime: Runtime, stream_alias: str) -> Source:
    return runtime.registries.stream_sources.get(stream_alias).stream()


def build_record_stream(runtime: Runtime, record_stream: Iterable[Mapping[str, Any]], stream_id: str) -> Iterator[TemporalRecord]:
    """Map dto's to TemporalRecord instances."""
    mapper = runtime.registries.mappers.get(stream_id)
    return mapper(record_stream)


def apply_record_operations(runtime: Runtime, record_stream: Iterable[TemporalRecord], stream_id: str) -> Iterator[TemporalRecord]:
    """Apply record transforms defined in contract policies in order."""
    steps = runtime.registries.record_operations.get(stream_id)
    records = apply_transforms(record_stream, RECORD_TRANSFORMS_EP, steps)
    return records


def build_feature_stream(
    record_stream: Iterable[TemporalRecord],
    base_feature_id: str,
    partition_by: Any | None = None,
) -> Iterator[FeatureRecord]:

    keygen = FeatureIdGenerator(partition_by)

    for rec in record_stream:
        yield FeatureRecord(
            record=rec,
            id=keygen.generate(base_feature_id, rec),
        )


def regularize_feature_stream(
    runtime: Runtime,
    feature_stream: Iterable[FeatureRecord],
    stream_id: str,
    batch_size: int,
) -> Iterator[FeatureRecord]:
    """Apply feature transforms defined in contract policies in order."""
    # Sort by (id, time) to satisfy stream transforms (ensure_ticks/fill)
    sorted = batch_sort(
        feature_stream,
        batch_size=batch_size,
        key=lambda fr: (fr.id, fr.record.time),
    )
    transformed = apply_transforms(
        sorted, STREAM_TRANFORMS_EP, runtime.registries.stream_operations.get(stream_id)
    )
    transformed = apply_transforms(
        transformed, DEBUG_TRANSFORMS_EP, runtime.registries.debug_operations.get(stream_id)
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


def vector_assemble_stage(
    merged: Iterator[FeatureRecord | FeatureRecordSequence],
    group_by_cadence: str,
) -> Iterator[Tuple[Any, Vector]]:
    """Group the merged feature stream by group_key.
    Coalesce each partitioned feature_id into record buckets.
    Yield (group_key, Vector) pairs ready for downstream consumption."""

    for group_key, group in groupby(
        merged, key=lambda fr: group_key_for(fr, group_by_cadence)
    ):
        feature_map = defaultdict(list)
        for fr in group:
            if isinstance(fr, FeatureRecordSequence):
                records = fr.records
            else:
                records = [fr.record]
            feature_map[fr.id].extend(records)
        yield group_key, vectorize_record_group(feature_map)


def post_process(
    runtime: Runtime,
    stream: Iterator[Tuple[Any, Vector]],
) -> Iterator[Tuple[Any, Vector]]:
    """Apply project-scoped postprocess transforms (from registry).

    Explicit prereq artifact flow:
    - Read a precomputed expected feature-id list (full ids) from the build
      folder. If missing, instruct the user to generate it via CLI.
    """
    transforms = runtime.registries.postprocesses.get(POSTPROCESS_TRANSFORMS)

    if not transforms:
        return stream

    expected_ids = load_artifact(runtime, PARTIONED_IDS)

    def _with_context() -> Iterator[Tuple[Any, Vector]]:
        token = set_expected_ids(expected_ids)
        try:
            yield from apply_transforms(stream, VECTOR_TRANSFORMS_EP, transforms)
        finally:
            reset_expected_ids(token)

    return _with_context()
