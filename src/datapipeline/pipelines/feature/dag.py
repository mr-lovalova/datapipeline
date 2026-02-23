from collections.abc import Iterator
from typing import Any, Mapping

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.dag.dag import StageDag
from datapipeline.dag.runner import run_stage_dag
from datapipeline.dag.node import PipelineStep
from datapipeline.pipelines.feature.nodes import (
    build_feature_stream,
    feature_transforms,
    order_feature_records,
)
from datapipeline.pipelines.record.dag import build_record_nodes
from datapipeline.dag.context import PipelineContext


def build_feature_pipeline(
    context: PipelineContext,
    cfg: FeatureRecordConfig,
    stage: int | None = None,
) -> Iterator[Any]:
    dag = StageDag(
        name=f"feature:{cfg.id}",
        metadata=_feature_dag_metadata(
            record_stream_id=cfg.record_stream,
            feature_id=cfg.id,
            field=cfg.field,
            scale=cfg.scale,
            sequence=cfg.sequence,
        ),
        nodes=(
            *build_record_nodes(context, cfg.record_stream),
            *build_feature_nodes(
                context,
                record_stream_id=cfg.record_stream,
                feature_id=cfg.id,
                field=cfg.field,
                scale=cfg.scale,
                sequence=cfg.sequence,
            ),
        ),
    ).upto_stage(stage)
    return run_stage_dag(context, dag)


def _feature_dag_metadata(
    record_stream_id: str,
    feature_id: str,
    field: str,
    scale: Mapping[str, Any] | bool | None,
    sequence: Mapping[str, Any] | None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "feature.config": {
            "id": feature_id,
            "stream": record_stream_id,
            "field": field,
        }
    }
    transforms: list[str] = []
    if scale:
        transforms.append("scale")
    if sequence:
        transforms.append("sequence")
    if transforms:
        metadata["feature.transforms"] = ",".join(transforms)
    return metadata


def build_feature_nodes(
    context: PipelineContext,
    record_stream_id: str,
    feature_id: str,
    field: str,
    scale: Mapping[str, Any] | bool | None,
    sequence: Mapping[str, Any] | None,
) -> tuple[PipelineStep, ...]:
    partition_by = context.runtime.registries.partition_by.get(record_stream_id)
    batch_size = context.runtime.registries.sort_batch_size.get(record_stream_id)
    return (
        PipelineStep(
            name="build_feature_stream",
            op=build_feature_stream,
            args=(feature_id, field, partition_by),
            input="stream_transforms",
        ),
        PipelineStep(
            name="feature_transforms",
            op=feature_transforms,
            args=(context, scale, sequence),
            input="build_feature_stream",
        ),
        PipelineStep(
            name="order_feature_records",
            op=order_feature_records,
            args=(batch_size,),
            input="feature_transforms",
        ),
    )
