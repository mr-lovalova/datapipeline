import logging
import shutil
from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from itertools import groupby
from pathlib import Path
from typing import Any
from uuid import uuid4

from datapipeline.artifacts.series import (
    SERIES_MANIFEST_VERSION,
    SeriesManifest,
    SeriesShard,
    series_record_to_row,
    write_series_rows,
)
from datapipeline.config.dataset.series import SeriesConfig
from datapipeline.config.tasks import SeriesTask
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.pipeline import Pipeline, Stage
from datapipeline.execution.runner import run_pipeline
from datapipeline.operations.persistence import ArtifactOutput
from datapipeline.pipelines.series.stages import SeriesSequencer
from datapipeline.pipelines.series.projector import SeriesProjector
from datapipeline.pipelines.sort import SortProgress, batch_sort
from datapipeline.pipelines.stream.pipeline import build_stream_pipeline
from datapipeline.runtime import Runtime, require_runtime_stream
from datapipeline.services.path_policy import resolve_artifact_output_path
from datapipeline.utils.json_artifact import write_json_artifact
from datapipeline.utils.time import floor_time_to_cadence, parse_cadence

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ShardPlan:
    ordinal: int
    config: SeriesConfig
    path: Path


@dataclass(frozen=True)
class _SeriesRow:
    ordinal: int
    order: tuple[Any, ...]
    payload: dict[str, Any]


def build_series_artifact(
    runtime: Runtime,
    task_cfg: SeriesTask,
) -> ArtifactOutput:
    dataset = runtime.dataset

    relative_path = Path(task_cfg.output)
    destination = resolve_artifact_output_path(relative_path, runtime.artifacts_root)
    cache_root = destination.parent / f"{destination.stem}.shards"
    if cache_root.is_symlink():
        raise ValueError("Series shard directory must not be a symlink.")
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
            SeriesShard(
                id=plan.config.id,
                path=str(
                    relative_generation_root / plan.path.relative_to(staging_root)
                ),
                rows=written_shards[plan.ordinal],
            )
            for plan in feature_plans
        )
        target_shards = tuple(
            SeriesShard(
                id=plan.config.id,
                path=str(
                    relative_generation_root / plan.path.relative_to(staging_root)
                ),
                rows=written_shards[plan.ordinal],
            )
            for plan in target_plans
        )

        manifest = SeriesManifest(
            version=SERIES_MANIFEST_VERSION,
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
            "Failed to remove incomplete series generation %s",
            generation_root,
            exc_info=True,
        )


def _materialize_stream_groups(
    context: PipelineContext,
    plans: Sequence[_ShardPlan],
    sample_key_contract: SampleKeyContract,
    cadence: str,
) -> dict[int, int]:
    plans_by_stream: dict[str, list[_ShardPlan]] = defaultdict(list)
    for plan in plans:
        plans_by_stream[plan.config.stream].append(plan)

    cadence_step = parse_cadence(cadence)
    rows: dict[int, int] = {}
    for stream_plans in plans_by_stream.values():
        rows.update(
            _materialize_stream_group(
                context,
                stream_plans,
                sample_key_contract,
                cadence_step,
            )
        )
    return rows


def _materialize_stream_group(
    context: PipelineContext,
    plans: Sequence[_ShardPlan],
    sample_key_contract: SampleKeyContract,
    cadence_step: timedelta,
) -> dict[int, int]:
    stream_id = plans[0].config.stream
    stream = require_runtime_stream(context.runtime, stream_id)
    configs = tuple(plan.config for plan in plans)
    projector = SeriesProjector(stream.partition_by, sample_key_contract)
    sequencers = {
        plan.ordinal: SeriesSequencer(plan.config.sequence)
        for plan in plans
        if plan.config.sequence is not None
    }

    def project_series(
        records: Iterator[Any],
    ) -> Iterator[_SeriesRow]:
        for record in records:
            for plan, projected in zip(
                plans,
                projector.project(record, configs),
                strict=True,
            ):
                sequencer = sequencers.get(plan.ordinal)
                result = projected if sequencer is None else sequencer.append(projected)
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
                    yield _SeriesRow(
                        ordinal=plan.ordinal,
                        order=order,
                        payload=series_record_to_row(result),
                    )

    record_pipeline = build_stream_pipeline(context, stream_id)
    sort_progress = SortProgress()
    pipeline = Pipeline(
        name=f"series:{stream_id}",
        input=record_pipeline.input,
        stages=(
            *record_pipeline.stages,
            Stage(
                name="project_series",
                apply=project_series,
            ),
            Stage(
                name="order_series",
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
    row_counts: dict[int, int] = {}
    try:
        for ordinal, rows in groupby(ordered, key=lambda row: row.ordinal):
            plan = plans_by_ordinal[ordinal]
            row_counts[ordinal] = write_series_rows(
                plan.path,
                (row.payload for row in rows),
            )
    finally:
        _close_iterator(ordered)

    for plan in plans:
        if plan.ordinal not in row_counts:
            row_counts[plan.ordinal] = write_series_rows(plan.path, ())
    return row_counts


def _close_iterator(items: Iterator[object]) -> None:
    closer = getattr(items, "close", None)
    if callable(closer):
        closer()
