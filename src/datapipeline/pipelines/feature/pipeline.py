from collections.abc import Iterator, Sequence
from dataclasses import replace
from functools import partial
from typing import Any

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.domain.sample_key import SampleKeyContract
from datapipeline.execution.node import PipelineNode
from datapipeline.execution.pipeline import Pipeline
from datapipeline.execution.runner import run_pipeline
from datapipeline.pipelines.feature.nodes import (
    build_feature_stream,
    order_feature_records,
    scale_features,
    sequence_features,
)
from datapipeline.pipelines.feature.projector import FeatureProjector
from datapipeline.execution.context import PipelineContext
from datapipeline.pipelines.stream.pipeline import build_stream_pipeline
from datapipeline.runtime import require_runtime_stream


def run_feature_pipeline(
    context: PipelineContext,
    cfg: FeatureRecordConfig,
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
) -> Iterator[Any]:
    return run_pipeline(
        context,
        build_feature_pipeline(
            context,
            cfg,
            sample_keys=sample_keys,
            group_by_cadence=group_by_cadence,
        ),
    )


def build_feature_pipeline(
    context: PipelineContext,
    cfg: FeatureRecordConfig,
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
) -> Pipeline:
    record_pipeline = build_stream_pipeline(context, cfg.stream)
    record_nodes = tuple(
        replace(node, name=f"{record_pipeline.name}/{node.name}")
        for node in record_pipeline.nodes
    )
    return Pipeline(
        name=f"feature:{cfg.id}",
        nodes=(
            *record_nodes,
            *build_feature_nodes(
                context,
                cfg,
                sample_keys=sample_keys,
                group_by_cadence=group_by_cadence,
            ),
        ),
        summary=record_pipeline.summary,
    )


def build_feature_nodes(
    context: PipelineContext,
    config: FeatureRecordConfig,
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
) -> tuple[PipelineNode, ...]:
    stream = require_runtime_stream(context.runtime, config.stream)
    projector = FeatureProjector(
        stream.partition_by,
        SampleKeyContract(sample_keys),
    )
    nodes = [
        PipelineNode(
            name="build_feature_stream",
            apply=partial(
                build_feature_stream,
                projector,
                config,
            ),
        ),
    ]
    if config.scale:
        nodes.append(
            PipelineNode(
                name="scale_features",
                apply=partial(scale_features, context),
            )
        )
    if config.sequence is not None:
        nodes.append(
            PipelineNode(
                name="sequence_features",
                apply=partial(sequence_features, config.sequence),
            )
        )
    nodes.append(
        PipelineNode(
            name="order_feature_records",
            apply=partial(
                order_feature_records,
                context,
                group_by_cadence,
                sample_keys,
            ),
        ),
    )
    return tuple(nodes)
