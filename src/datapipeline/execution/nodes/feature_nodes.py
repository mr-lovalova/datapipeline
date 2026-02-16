from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from datapipeline.config.dataset.feature import FeatureRecordConfig
from datapipeline.execution.nodes.spec import PipelineNode
from datapipeline.pipeline.stages import (
    apply_feature_transforms,
    apply_record_operations,
    apply_stream_operations,
    build_feature_stream,
    build_record_stream,
    open_source_stream,
    order_record_stream,
)
from datapipeline.pipeline.utils.memory_sort import batch_sort

RECORD_NODE_COUNT = 5
FEATURE_NODE_COUNT = 3


def _time_then_id(item: Any) -> tuple[Any, Any]:
    rec = getattr(item, "record", None)
    if rec is not None:
        time_value = getattr(rec, "time", None)
    else:
        records = getattr(item, "records", None)
        time_value = getattr(records[0], "time", None) if records else None
    return time_value, getattr(item, "id", None)


def build_record_nodes(record_stream_id: str) -> tuple[PipelineNode, ...]:
    def _open_source(context, _):
        return open_source_stream(context, record_stream_id)

    def _map_records(context, rows):
        return build_record_stream(context, _require(rows), record_stream_id)

    def _apply_record_operations(context, records):
        return apply_record_operations(context, _require(records), record_stream_id)

    def _order_records(context, records):
        batch_size = context.runtime.registries.sort_batch_size.get(record_stream_id)
        return order_record_stream(
            context,
            _require(records),
            record_stream_id,
            batch_size,
        )

    def _apply_stream_operations(context, records):
        return apply_stream_operations(context, _require(records), record_stream_id)

    return (
        PipelineNode(name="open_source", run=_open_source),
        PipelineNode(name="map_records", run=_map_records),
        PipelineNode(name="record_transforms", run=_apply_record_operations),
        PipelineNode(name="order_records", run=_order_records),
        PipelineNode(name="stream_transforms", run=_apply_stream_operations),
    )


def build_feature_nodes(
    cfg: FeatureRecordConfig,
    *,
    batch_size: int,
    partition_by: str | list[str] | None,
) -> tuple[PipelineNode, ...]:
    def _build_feature_records(_context, records):
        return build_feature_stream(
            _require(records),
            base_feature_id=cfg.id,
            field=cfg.field,
            partition_by=partition_by,
        )

    def _apply_feature_transforms(context, features):
        return apply_feature_transforms(
            context,
            _require(features),
            cfg.scale,
            cfg.sequence,
        )

    def _order_feature_records(_context, features):
        return batch_sort(
            _require(features),
            batch_size=batch_size,
            key=_time_then_id,
        )

    return (
        PipelineNode(name="build_feature_stream", run=_build_feature_records),
        PipelineNode(name="feature_transforms", run=_apply_feature_transforms),
        PipelineNode(name="order_feature_records", run=_order_feature_records),
    )


def _require(stream: Iterable[Any] | None) -> Iterable[Any]:
    if stream is None:
        return iter(())
    return stream
