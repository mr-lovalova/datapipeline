from collections.abc import Iterator, Sequence
from dataclasses import replace
from functools import partial

from datapipeline.config.dataset.series import SeriesConfig
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.domain.series import SeriesRecord, SeriesSequence
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import PipelineNode
from datapipeline.execution.pipeline import Pipeline
from datapipeline.execution.runner import run_pipeline
from datapipeline.pipelines.series.nodes import (
    order_series,
    project_series,
    sequence_series,
)
from datapipeline.pipelines.series.projector import SeriesProjector
from datapipeline.pipelines.sort import SortProgress
from datapipeline.pipelines.stream.pipeline import build_stream_pipeline
from datapipeline.runtime import require_runtime_stream


def run_series_pipeline(
    context: PipelineContext,
    cfg: SeriesConfig,
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
) -> Iterator[SeriesRecord | SeriesSequence]:
    return run_pipeline(
        context,
        build_series_pipeline(
            context,
            cfg,
            sample_keys=sample_keys,
            group_by_cadence=group_by_cadence,
        ),
    )


def build_series_pipeline(
    context: PipelineContext,
    cfg: SeriesConfig,
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
) -> Pipeline:
    record_pipeline = build_stream_pipeline(context, cfg.stream)
    record_nodes = tuple(
        replace(node, name=f"{record_pipeline.name}/{node.name}")
        for node in record_pipeline.nodes
    )
    return Pipeline(
        name=f"series:{cfg.id}",
        nodes=(
            *record_nodes,
            *build_series_nodes(
                context,
                cfg,
                sample_keys=sample_keys,
                group_by_cadence=group_by_cadence,
            ),
        ),
        summary=record_pipeline.summary,
    )


def build_series_nodes(
    context: PipelineContext,
    config: SeriesConfig,
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
) -> tuple[PipelineNode, ...]:
    stream = require_runtime_stream(context.runtime, config.stream)
    projector = SeriesProjector(
        stream.partition_by,
        SampleKeyContract(sample_keys),
    )
    nodes = [
        PipelineNode(
            name="project_series",
            apply=partial(
                project_series,
                projector,
                config,
            ),
        ),
    ]
    if config.sequence is not None:
        nodes.append(
            PipelineNode(
                name="sequence_series",
                apply=partial(sequence_series, config.sequence),
            )
        )
    if stream.partition_by or sample_keys:
        sort_progress = SortProgress()
        nodes.append(
            PipelineNode(
                name="order_series",
                apply=partial(
                    order_series,
                    context.runtime.execution.sort_buffer_bytes,
                    group_by_cadence,
                    sample_keys,
                    sort_progress,
                ),
                progress=sort_progress.snapshot,
            ),
        )
    return tuple(nodes)
