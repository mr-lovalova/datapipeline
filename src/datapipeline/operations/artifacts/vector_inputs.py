import logging
import shutil
from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from functools import partial
from itertools import groupby
from pathlib import Path
from typing import Any
from uuid import uuid4

from datapipeline.artifacts.registry import SCALER_SPEC
from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.tasks import VectorInputsTask
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import PipelineNode
from datapipeline.execution.pipeline import Pipeline
from datapipeline.execution.runner import run_pipeline
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.pipelines.feature.nodes import FeatureSequencer
from datapipeline.pipelines.feature.projector import FeatureProjector
from datapipeline.pipelines.sort import SortProgress, batch_sort
from datapipeline.pipelines.stream.pipeline import build_stream_pipeline
from datapipeline.runtime import Runtime, require_runtime_stream
from datapipeline.services.path_policy import resolve_artifact_output_path
from datapipeline.transforms.feature.scaler import FeatureScaler
from datapipeline.utils.json_artifact import write_json_artifact
from datapipeline.utils.time import floor_time_to_cadence, parse_cadence
from datapipeline.vector_inputs.store import (
    VECTOR_INPUTS_MANIFEST_VERSION,
    CachedVectorInputShard,
    CachedVectorInputsManifest,
    WrittenVectorInputShard,
    feature_record_to_vector_input_row,
    write_vector_input_rows,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ShardPlan:
    ordinal: int
    config: FeatureRecordConfig
    path: Path


@dataclass(frozen=True)
class _VectorInputRow:
    ordinal: int
    order: tuple[Any, ...]
    payload: dict[str, Any]


def materialize_vector_inputs(
    runtime: Runtime,
    task_cfg: VectorInputsTask,
) -> ArtifactOutput:
    dataset = runtime.dataset

    relative_path = Path(task_cfg.output)
    destination = resolve_artifact_output_path(relative_path, runtime.artifacts_root)
    cache_root = destination.parent / f"{destination.stem}.shards"
    if cache_root.is_symlink():
        raise ValueError("Vector inputs shard directory must not be a symlink.")
    generation = uuid4().hex
    staging_root = cache_root / f".staging-{generation}"
    generation_root = cache_root / generation

    context = PipelineContext(runtime)
    sample_key_contract = SampleKeyContract(dataset.sample.keys)
    feature_dir = staging_root / "features"
    target_dir = staging_root / "targets"

    feature_plans = tuple(
        _ShardPlan(
            ordinal=index,
            config=config,
            path=feature_dir / f"{index:06d}.jsonl.gz",
        )
        for index, config in enumerate(dataset.features)
    )
    target_offset = len(feature_plans)
    target_plans = tuple(
        _ShardPlan(
            ordinal=target_offset + index,
            config=config,
            path=target_dir / f"{index:06d}.jsonl.gz",
        )
        for index, config in enumerate(dataset.targets)
    )

    try:
        feature_dir.mkdir(parents=True)
        target_dir.mkdir()
        written_shards = _materialize_stream_groups(
            context,
            (*feature_plans, *target_plans),
            sample_key_contract,
            dataset.sample.cadence,
        )
        relative_generation_root = Path(cache_root.name) / generation

        feature_shards = tuple(
            CachedVectorInputShard(
                id=plan.config.id,
                path=str(
                    relative_generation_root / plan.path.relative_to(staging_root)
                ),
                rows=written_shards[plan.ordinal].rows,
            )
            for plan in feature_plans
        )
        target_shards = tuple(
            CachedVectorInputShard(
                id=plan.config.id,
                path=str(
                    relative_generation_root / plan.path.relative_to(staging_root)
                ),
                rows=written_shards[plan.ordinal].rows,
            )
            for plan in target_plans
        )

        manifest = CachedVectorInputsManifest(
            version=VECTOR_INPUTS_MANIFEST_VERSION,
            cadence=dataset.sample.cadence,
            sample_keys=tuple(dataset.sample.keys),
            sample_key_types=sample_key_contract.types,
            features=feature_shards,
            targets=target_shards,
        )
        staging_root.rename(generation_root)
    except BaseException:
        _remove_failed_generation(staging_root)
        raise

    try:
        write_json_artifact(destination, manifest.model_dump(mode="json"))
    except BaseException:
        _remove_failed_generation(generation_root)
        raise

    feature_rows = sum(shard.rows for shard in feature_shards)
    target_rows = sum(shard.rows for shard in target_shards)
    return ArtifactOutput(
        relative_path=str(relative_path),
        companion_paths=tuple(
            str(relative_path.parent / shard.path)
            for shard in (*feature_shards, *target_shards)
        ),
        meta={
            "features": len(feature_shards),
            "targets": len(target_shards),
            "feature_rows": feature_rows,
            "target_rows": target_rows,
            "format": manifest.format,
        },
    )


def _remove_failed_generation(generation_root: Path) -> None:
    try:
        shutil.rmtree(generation_root)
    except FileNotFoundError:
        pass
    except OSError:
        logger.warning(
            "Failed to remove incomplete vector-input generation %s",
            generation_root,
            exc_info=True,
        )


def _materialize_stream_groups(
    context: PipelineContext,
    plans: Sequence[_ShardPlan],
    sample_key_contract: SampleKeyContract,
    cadence: str,
) -> dict[int, WrittenVectorInputShard]:
    plans_by_stream: dict[str, list[_ShardPlan]] = defaultdict(list)
    for plan in plans:
        plans_by_stream[plan.config.stream].append(plan)

    scaler = None
    if any(plan.config.scale for plan in plans):
        scaler = FeatureScaler(context.require_artifact(SCALER_SPEC))

    rows: dict[int, WrittenVectorInputShard] = {}
    for stream_plans in plans_by_stream.values():
        rows.update(
            _materialize_stream_group(
                context,
                stream_plans,
                sample_key_contract,
                cadence,
                scaler,
            )
        )
    return rows


def _materialize_stream_group(
    context: PipelineContext,
    plans: Sequence[_ShardPlan],
    sample_key_contract: SampleKeyContract,
    cadence: str,
    scaler: FeatureScaler | None,
) -> dict[int, WrittenVectorInputShard]:
    stream_id = plans[0].config.stream
    stream = require_runtime_stream(context.runtime, stream_id)
    configs = tuple(plan.config for plan in plans)
    projector = FeatureProjector(stream.partition_by, sample_key_contract)
    sequencers = {
        plan.ordinal: FeatureSequencer(plan.config.sequence)
        for plan in plans
        if plan.config.sequence is not None
    }

    cadence_step = parse_cadence(cadence)

    def build_vector_inputs(
        records: Iterator[Any],
    ) -> Iterator[_VectorInputRow]:
        for record in records:
            for plan, feature in zip(
                plans,
                projector.project(record, configs),
                strict=True,
            ):
                config = plan.config
                if config.scale:
                    if scaler is None:
                        raise RuntimeError(
                            "Scaled vector inputs require a scaler artifact."
                        )
                    feature = scaler.scale(feature)
                sequencer = sequencers.get(plan.ordinal)
                result = feature if sequencer is None else sequencer.append(feature)
                if result is not None:
                    if sample_key_contract.fields:
                        order = (
                            plan.ordinal,
                            floor_time_to_cadence(result.time, cadence_step),
                            *result.entity_key,
                            result.time,
                            result.id,
                        )
                    else:
                        order = plan.ordinal, result.time, result.id
                    yield _VectorInputRow(
                        ordinal=plan.ordinal,
                        order=order,
                        payload=feature_record_to_vector_input_row(result),
                    )

    record_pipeline = build_stream_pipeline(context, stream_id)
    sort_progress = SortProgress()
    pipeline = Pipeline(
        name=f"vector_inputs:{stream_id}",
        nodes=(
            *record_pipeline.nodes,
            PipelineNode(
                name="build_vector_inputs",
                apply=build_vector_inputs,
            ),
            PipelineNode(
                name="order_vector_inputs",
                apply=partial(
                    batch_sort,
                    buffer_bytes=context.runtime.execution.sort_buffer_bytes,
                    key=lambda row: row.order,
                    progress=sort_progress,
                ),
                progress=sort_progress.snapshot,
            ),
        ),
        summary=record_pipeline.summary,
    )
    ordered = run_pipeline(context, pipeline)
    plans_by_ordinal = {plan.ordinal: plan for plan in plans}
    row_counts: dict[int, WrittenVectorInputShard] = {}
    try:
        for ordinal, rows in groupby(ordered, key=lambda row: row.ordinal):
            plan = plans_by_ordinal[ordinal]
            row_counts[ordinal] = write_vector_input_rows(
                plan.path,
                (row.payload for row in rows),
            )
    finally:
        _close_iterator(ordered)

    for plan in plans:
        if plan.ordinal not in row_counts:
            row_counts[plan.ordinal] = write_vector_input_rows(plan.path, ())
    return row_counts


def _close_iterator(items: Iterator[object]) -> None:
    closer = getattr(items, "close", None)
    if callable(closer):
        closer()
