from collections.abc import Iterator
from typing import Any

from datapipeline.dag.context import PipelineContext
from datapipeline.dag.dag import Dag
from datapipeline.dag.node import PipelineNode
from datapipeline.dag.runner import run_dag
from datapipeline.pipelines.ingest import build_ingest_dag, build_ingest_nodes
from datapipeline.pipelines.stream import build_stream_dag, build_stream_nodes


def build_stream_id_pipeline(
    context: PipelineContext,
    stream_id: str,
    node: int | None = None,
) -> Iterator[Any]:
    dag = build_stream_id_dag(context, stream_id).upto_node(node)
    return run_dag(context, dag)


def build_stream_id_dag(
    context: PipelineContext,
    stream_id: str,
) -> Dag:
    if _stream_pipeline(context, stream_id) == "ingest":
        return build_ingest_dag(context, stream_id)
    return build_stream_dag(context, stream_id)


def build_stream_id_nodes(
    context: PipelineContext,
    stream_id: str,
) -> tuple[PipelineNode, ...]:
    if _stream_pipeline(context, stream_id) == "ingest":
        return build_ingest_nodes(context, stream_id)
    return build_stream_nodes(context, stream_id)


def _stream_pipeline(context: PipelineContext, stream_id: str):
    try:
        return context.runtime.registries.stream_specs.get(stream_id).pipeline
    except KeyError as exc:
        available = sorted(context.runtime.registries.stream_specs.keys())
        available_text = ", ".join(available) if available else "(none)"
        raise KeyError(
            f"Unknown stream '{stream_id}'. Check dataset.yaml and stream ids. "
            f"Available streams: {available_text}"
        ) from exc
