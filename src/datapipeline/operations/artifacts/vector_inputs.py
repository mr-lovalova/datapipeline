import logging
import gzip
import hashlib
import json
import shutil
import zlib
from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from functools import partial
from itertools import groupby
from pathlib import Path
from typing import Any
from uuid import uuid4

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.config.tasks import VectorInputsTask
from datapipeline.domain.feature import FeatureRecord, FeatureSequence
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
from datapipeline.services.artifacts import SCALER_SPEC
from datapipeline.services.path_policy import resolve_artifact_output_path
from datapipeline.transforms.feature.scaler import FeatureScaler
from datapipeline.utils.json_artifact import write_json_artifact
from datapipeline.utils.time import floor_time_to_cadence, parse_cadence
from datapipeline.vector_inputs.store import (
    CachedVectorInputShard,
    CachedVectorInputsManifest,
    VECTOR_INPUTS_MANIFEST_VERSION,
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
    staging_root = cache_root / f".staging-{uuid4().hex}"

    context = PipelineContext(runtime)
    sample_key_contract = SampleKeyContract(dataset.sample.keys)
    feature_dir = staging_root / "features"
    target_dir = staging_root / "targets"
    feature_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)

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
        written_shards = _materialize_stream_groups(
            context,
            (*feature_plans, *target_plans),
            sample_key_contract,
            dataset.sample.cadence,
        )
        generation = _generation_name(
            dataset.sample.cadence,
            sample_key_contract,
            feature_plans,
            target_plans,
            written_shards,
        )
        generation_root = cache_root / generation
        _publish_generation(
            staging_root,
            generation_root,
            (*feature_plans, *target_plans),
            written_shards,
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
        write_json_artifact(destination, manifest.model_dump(mode="json"))
    except BaseException:
        _remove_failed_generation(staging_root)
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


def _generation_name(
    cadence: str,
    sample_keys: SampleKeyContract,
    feature_plans: Sequence[_ShardPlan],
    target_plans: Sequence[_ShardPlan],
    written: dict[int, WrittenVectorInputShard],
) -> str:
    def entries(plans: Sequence[_ShardPlan]) -> list[dict[str, object]]:
        return [
            {
                "id": plan.config.id,
                "rows": written[plan.ordinal].rows,
                "content_hash": written[plan.ordinal].content_hash,
            }
            for plan in plans
        ]

    contract = {
        "version": VECTOR_INPUTS_MANIFEST_VERSION,
        "format": "jsonl.gz",
        "cadence": cadence,
        "sample_keys": sample_keys.fields,
        "sample_key_types": sample_keys.types,
        "features": entries(feature_plans),
        "targets": entries(target_plans),
    }
    payload = json.dumps(
        contract,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _publish_generation(
    staging_root: Path,
    generation_root: Path,
    plans: Sequence[_ShardPlan],
    written: dict[int, WrittenVectorInputShard],
) -> None:
    try:
        staging_root.rename(generation_root)
        return
    except OSError:
        if not generation_root.exists():
            raise

    if _generation_matches(staging_root, generation_root, plans, written):
        _remove_failed_generation(staging_root)
        return

    quarantine = generation_root.with_name(
        f".corrupt-{generation_root.name}-{uuid4().hex}"
    )
    generation_root.rename(quarantine)
    try:
        staging_root.rename(generation_root)
    except BaseException:
        if not generation_root.exists():
            quarantine.rename(generation_root)
        raise


def _generation_matches(
    staging_root: Path,
    generation_root: Path,
    plans: Sequence[_ShardPlan],
    written: dict[int, WrittenVectorInputShard],
) -> bool:
    if generation_root.is_symlink() or not generation_root.is_dir():
        return False
    expected_paths = {plan.path.relative_to(staging_root) for plan in plans}
    actual_paths = {
        path.relative_to(generation_root)
        for path in generation_root.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    if actual_paths != expected_paths:
        return False
    for plan in plans:
        published_path = generation_root / plan.path.relative_to(staging_root)
        if published_path.is_symlink() or not published_path.is_file():
            return False
        digest = hashlib.sha256()
        try:
            with gzip.open(published_path, "rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
        except (EOFError, gzip.BadGzipFile, zlib.error):
            return False
        if digest.hexdigest() != written[plan.ordinal].content_hash:
            return False
    return True


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

    def build_tagged_features(
        records: Iterator[Any],
    ) -> Iterator[tuple[int, FeatureRecord | FeatureSequence]]:
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
                    yield plan.ordinal, result

    cadence_step = parse_cadence(cadence)

    def order_key(
        tagged: tuple[int, FeatureRecord | FeatureSequence],
    ) -> tuple[Any, ...]:
        ordinal, item = tagged
        if sample_key_contract.fields:
            return (
                ordinal,
                floor_time_to_cadence(item.time, cadence_step),
                *item.entity_key,
                item.time,
                item.id,
            )
        return ordinal, item.time, item.id

    record_pipeline = build_stream_pipeline(context, stream_id)
    sort_progress = SortProgress()
    pipeline = Pipeline(
        name=f"vector_inputs:{stream_id}",
        nodes=(
            *record_pipeline.nodes,
            PipelineNode(
                name="build_vector_inputs",
                apply=build_tagged_features,
            ),
            PipelineNode(
                name="order_vector_inputs",
                apply=partial(
                    batch_sort,
                    buffer_bytes=context.runtime.execution.sort_buffer_bytes,
                    key=order_key,
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
        for ordinal, tagged_group in groupby(ordered, key=lambda tagged: tagged[0]):
            plan = plans_by_ordinal[ordinal]
            row_counts[ordinal] = write_vector_input_rows(
                plan.path,
                (feature_record_to_vector_input_row(item) for _, item in tagged_group),
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
