from collections.abc import Iterator, Sequence
from typing import Any, Mapping

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.dag.dag import Dag
from datapipeline.dag.runner import run_dag
from datapipeline.dag.node import PipelineNode
from datapipeline.pipelines.feature.nodes import (
    build_feature_stream,
    feature_transforms,
    order_feature_records,
)
from datapipeline.pipelines.stream_id import build_stream_id_nodes
from datapipeline.pipelines.record.streams import open_record_stream
from datapipeline.dag.context import PipelineContext


def build_feature_pipeline(
    context: PipelineContext,
    cfg: FeatureRecordConfig,
    node: int | None = None,
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
) -> Iterator[Any]:
    if node is None:
        record_stream = open_record_stream(context, cfg.record_stream)
        return run_dag(
            context,
            build_feature_dag(
                context,
                cfg,
                include_record_nodes=False,
                sample_keys=sample_keys,
                group_by_cadence=group_by_cadence,
            ),
            seed=record_stream,
        )

    return run_dag(
        context,
        build_feature_dag(
            context,
            cfg,
            include_record_nodes=True,
            sample_keys=sample_keys,
            group_by_cadence=group_by_cadence,
        ).upto_node(node),
    )


def build_feature_dag(
    context: PipelineContext,
    cfg: FeatureRecordConfig,
    *,
    include_record_nodes: bool = False,
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
) -> Dag:
    metadata = _feature_dag_metadata(
        record_stream_id=cfg.record_stream,
        feature_id=cfg.id,
        field=cfg.field,
        scale=cfg.scale,
        sequence=cfg.sequence,
    )
    record_nodes = (
        build_stream_id_nodes(context, cfg.record_stream) if include_record_nodes else ()
    )
    record_input = (
        _record_node_output(context, cfg.record_stream)
        if include_record_nodes
        else "seed"
    )
    return Dag(
        name=f"feature:{cfg.id}",
        metadata=metadata,
        nodes=(
            *record_nodes,
            *build_feature_nodes(
                context,
                record_stream_id=cfg.record_stream,
                feature_id=cfg.id,
                field=cfg.field,
                scale=cfg.scale,
                sequence=cfg.sequence,
                sample_keys=sample_keys,
                group_by_cadence=group_by_cadence,
                record_input=record_input,
            ),
        ),
    )


def _record_node_output(context: PipelineContext, record_stream_id: str) -> str:
    pipeline = context.runtime.registries.stream_specs.get(record_stream_id).pipeline
    if pipeline == "ingest":
        return "ordered"
    return "stream_transforms"


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
    sample_keys: Sequence[str] = (),
    group_by_cadence: str | None = None,
    record_input: str = "stream_transforms",
) -> tuple[PipelineNode, ...]:
    partition_by = context.runtime.registries.partition_by.get(record_stream_id)
    batch_size = context.runtime.registries.sort_batch_size.get(record_stream_id)
    return (
        PipelineNode(
            name="build_feature_stream",
            op=build_feature_stream,
            args=(feature_id, field, partition_by, sample_keys),
            input=record_input,
        ),
        PipelineNode(
            name="feature_transforms",
            op=feature_transforms,
            args=(context, scale, sequence),
            input="build_feature_stream",
        ),
        PipelineNode(
            name="order_feature_records",
            op=order_feature_records,
            args=(
                context,
                batch_size,
                f"Ordering feature {feature_id}",
                group_by_cadence,
            ),
            input="feature_transforms",
        ),
    )
