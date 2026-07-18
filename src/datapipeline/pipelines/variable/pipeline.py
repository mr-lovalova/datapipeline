from collections.abc import Iterator, Sequence
from dataclasses import replace
from functools import partial

from datapipeline.config.dataset.variable import VariableConfig
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.domain.variable import VariableRecord, VariableSequence
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.node import PipelineNode
from datapipeline.execution.pipeline import Pipeline
from datapipeline.execution.runner import run_pipeline
from datapipeline.pipelines.variable.nodes import (
    order_variable_records,
    project_variable_records,
    sequence_variables,
)
from datapipeline.pipelines.variable.projector import VariableProjector
from datapipeline.pipelines.sort import SortProgress
from datapipeline.pipelines.stream.pipeline import build_stream_pipeline
from datapipeline.runtime import require_runtime_stream


def run_variable_pipeline(
    context: PipelineContext,
    cfg: VariableConfig,
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
) -> Iterator[VariableRecord | VariableSequence]:
    return run_pipeline(
        context,
        build_variable_pipeline(
            context,
            cfg,
            sample_keys=sample_keys,
            group_by_cadence=group_by_cadence,
        ),
    )


def build_variable_pipeline(
    context: PipelineContext,
    cfg: VariableConfig,
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
) -> Pipeline:
    record_pipeline = build_stream_pipeline(context, cfg.stream)
    record_nodes = tuple(
        replace(node, name=f"{record_pipeline.name}/{node.name}")
        for node in record_pipeline.nodes
    )
    return Pipeline(
        name=f"variable:{cfg.id}",
        nodes=(
            *record_nodes,
            *build_variable_nodes(
                context,
                cfg,
                sample_keys=sample_keys,
                group_by_cadence=group_by_cadence,
            ),
        ),
        summary=record_pipeline.summary,
    )


def build_variable_nodes(
    context: PipelineContext,
    config: VariableConfig,
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
) -> tuple[PipelineNode, ...]:
    stream = require_runtime_stream(context.runtime, config.stream)
    projector = VariableProjector(
        stream.partition_by,
        SampleKeyContract(sample_keys),
    )
    nodes = [
        PipelineNode(
            name="project_variable_records",
            apply=partial(
                project_variable_records,
                projector,
                config,
            ),
        ),
    ]
    if config.sequence is not None:
        nodes.append(
            PipelineNode(
                name="sequence_variables",
                apply=partial(sequence_variables, config.sequence),
            )
        )
    if stream.partition_by or sample_keys:
        sort_progress = SortProgress()
        nodes.append(
            PipelineNode(
                name="order_variable_records",
                apply=partial(
                    order_variable_records,
                    context.runtime.execution.sort_buffer_bytes,
                    group_by_cadence,
                    sample_keys,
                    sort_progress,
                ),
                progress=sort_progress.snapshot,
            ),
        )
    return tuple(nodes)
