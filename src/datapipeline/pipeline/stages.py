from collections import defaultdict
from itertools import chain, groupby
from typing import Any, Iterable, Iterator, Mapping
from datetime import datetime

from datapipeline.pipeline.context import PipelineContext
from datapipeline.services.constants import POSTPROCESS_TRANSFORMS, SCALER_STATISTICS

from datapipeline.domain.feature import FeatureRecord, FeatureRecordSequence
from datapipeline.domain.vector import Vector, vectorize_record_group
from datapipeline.domain.sample import Sample
from datapipeline.pipeline.utils.memory_sort import batch_sort

from datapipeline.pipeline.utils.transform_utils import apply_transforms
from datapipeline.plugins import FEATURE_TRANSFORMS_EP, VECTOR_TRANSFORMS_EP, RECORD_TRANSFORMS_EP, STREAM_TRANFORMS_EP, DEBUG_TRANSFORMS_EP

from datapipeline.domain.record import TemporalRecord
from datapipeline.pipeline.utils.keygen import FeatureIdGenerator, group_key_for
from datapipeline.sources.models.source import Source
from datapipeline.transforms.vector import VectorEnsureSchemaTransform
from datapipeline.config.dataset.normalize import floor_time_to_bucket
from datapipeline.utils.time import parse_timecode


def open_source_stream(context: PipelineContext, stream_alias: str) -> Source:
    runtime = context.runtime
    return runtime.registries.stream_sources.get(stream_alias).stream()


def build_record_stream(
    context: PipelineContext,
    record_stream: Iterable[Mapping[str, Any]],
    stream_id: str,
) -> Iterator[TemporalRecord]:
    """Map dto's to TemporalRecord instances."""
    mapper = context.runtime.registries.mappers.get(stream_id)
    return mapper(record_stream)


def apply_record_operations(
    context: PipelineContext,
    record_stream: Iterable[TemporalRecord],
    stream_id: str,
) -> Iterator[TemporalRecord]:
    """Apply record transforms defined in contract policies in order."""
    steps = context.runtime.registries.record_operations.get(stream_id)
    records = apply_transforms(
        record_stream, RECORD_TRANSFORMS_EP, steps, context)
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
    context: PipelineContext,
    feature_stream: Iterable[FeatureRecord],
    stream_id: str,
    batch_size: int,
) -> Iterator[FeatureRecord]:
    """Apply feature transforms defined in contract policies in order."""
    # Sort by (id, time) to satisfy stream transforms (ensure_cadence/fill)
    sorted = batch_sort(
        feature_stream,
        batch_size=batch_size,
        key=lambda fr: (fr.id, fr.record.time),
    )
    transformed = apply_transforms(
        sorted,
        STREAM_TRANFORMS_EP,
        context.runtime.registries.stream_operations.get(stream_id),
        context,
    )
    transformed = apply_transforms(
        transformed,
        DEBUG_TRANSFORMS_EP,
        context.runtime.registries.debug_operations.get(stream_id),
        context,
    )
    return transformed


def apply_feature_transforms(
    context: PipelineContext,
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
        if "model_path" not in scale_args:
            if not context.artifacts.has(SCALER_STATISTICS):
                raise RuntimeError(
                    "Scaler artifact is missing. Run `jerry build` to generate it "
                    "or disable scale in feature config."
                )
            model_path = context.artifacts.resolve_path(SCALER_STATISTICS)
            scale_args["model_path"] = str(model_path)
        clauses.append({"scale": scale_args})

    if sequence:
        clauses.append({"sequence": dict(sequence)})

    transformed = apply_transforms(
        feature_stream, FEATURE_TRANSFORMS_EP, clauses, context)
    return transformed


def vector_assemble_stage(
    merged: Iterator[FeatureRecord | FeatureRecordSequence],
    group_by_cadence: str,
) -> Iterator[tuple[tuple, Vector]]:
    """Group merged feature stream by key and emit raw vectors."""
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
        vector = vectorize_record_group(feature_map)
        yield group_key, vector


def window_keys(start: datetime | None, end: datetime | None, cadence: str | None) -> Iterator[tuple] | None:
    if start is None or end is None or cadence is None:
        return None
    try:
        current = floor_time_to_bucket(start, cadence)
        stop = floor_time_to_bucket(end, cadence)
        step = parse_timecode(cadence)
    except Exception:
        return None
    if stop < current:
        return None

    def _iter():
        t = current
        while t <= stop:
            yield (t,)
            t = t + step

    return _iter()


def align_stream(
    stream: Iterator[tuple[tuple, Vector]] | None,
    keys: Iterator[tuple] | None,
) -> Iterator[tuple[tuple, Vector]]:
    if keys is None:
        return iter(stream or ())
    it = iter(stream or ())
    current = next(it, None)
    for key in keys:
        while current and current[0] < key:
            current = next(it, None)
        if current and current[0] == key:
            yield current
            current = next(it, None)
        else:
            yield (key, Vector(values={}))


def sample_assemble_stage(
    feature_vectors: Iterator[tuple[tuple, Vector]],
    target_vectors: Iterator[tuple[tuple, Vector]] | None = None,
) -> Iterator[Sample]:
    """Combine feature/target vectors into Sample objects."""
    feature_iter = iter(feature_vectors)
    target_iter = iter(target_vectors or ())

    def _advance(it):
        try:
            return next(it)
        except StopIteration:
            return None

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


def post_process(
    context: PipelineContext,
    stream: Iterator[Sample],
) -> Iterator[Sample]:
    """Apply project-scoped postprocess transforms (from registry).

    Explicit prereq artifact flow:
    - Read a precomputed expected feature-id list (full ids) from the build
      folder. If missing, instruct the user to generate it via CLI.
    """
    stream = _apply_vector_schema(context, stream)
    runtime = context.runtime
    transforms = runtime.registries.postprocesses.get(POSTPROCESS_TRANSFORMS)
    if not transforms:
        return stream
    return apply_transforms(stream, VECTOR_TRANSFORMS_EP, transforms, context)


def _apply_vector_schema(
    context: PipelineContext,
    stream: Iterator[Sample],
) -> Iterator[Sample]:
    with context.activate():
        feature_entries = context.load_schema(payload="features")
        target_entries = context.load_schema(payload="targets")

        if not feature_entries:
            if context.schema_required:
                raise RuntimeError("Schema missing for payload 'features'. Run `jerry build` to materialize schema.json.")
            feature_stream = stream
        else:
            feature_schema = VectorEnsureSchemaTransform(on_missing="fill", on_extra="drop")
            feature_schema.bind_context(context)
            feature_stream = feature_schema(stream)

        def _apply_targets(upstream: Iterator[Sample]) -> Iterator[Sample]:
            if target_entries:
                target_schema = VectorEnsureSchemaTransform(payload="targets", on_missing="fill", on_extra="drop")
                target_schema.bind_context(context)
                return target_schema(upstream)
            if not context.schema_required:
                return upstream
            # schema required but missing: only raise if targets are present in stream
            iterator = iter(upstream)
            try:
                first = next(iterator)
            except StopIteration:
                return iter(())
            if first.targets is None:
                return chain([first], iterator)
            raise RuntimeError("Schema missing for payload 'targets'. Run `jerry build` to materialize schema.json.")

        return _apply_targets(feature_stream)
