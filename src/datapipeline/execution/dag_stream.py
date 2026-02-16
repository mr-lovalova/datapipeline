from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Iterator
from typing import Any

from datapipeline.execution.observer import (
    ExecutionObserver,
    LoggingExecutionObserver,
    NoopExecutionObserver,
)
from datapipeline.execution.run_events import DagRunEvent, NodeRunEvent
from datapipeline.execution.stage_dag import StageDag
from datapipeline.pipeline.context import PipelineContext

logger = logging.getLogger(__name__)
_LOG_OBSERVER = LoggingExecutionObserver(logger)
_NOOP_OBSERVER = NoopExecutionObserver()


def run_stage_dag(
    context: PipelineContext,
    dag: StageDag,
    *,
    seed: Iterable[Any] | None = None,
    observer: ExecutionObserver | None = None,
) -> Iterator[Any]:
    active_observer = _resolve_observer(context, observer)
    stream: Iterable[Any] | None = seed
    for index, node in enumerate(dag.nodes):
        produced = node.run(context, stream)
        stream = _observe_node_stream(
            dag_name=dag.name,
            node_name=node.name,
            stage=index,
            stream=produced,
            observer=active_observer,
        )
    return _observe_dag_stream(
        dag=dag,
        stream=stream,
        observer=active_observer,
    )


def _resolve_observer(
    context: PipelineContext,
    observer: ExecutionObserver | None,
) -> ExecutionObserver:
    if observer is not None:
        return observer
    context_observer = getattr(context, "execution_observer", None)
    if context_observer is not None:
        return context_observer
    if logger.isEnabledFor(logging.DEBUG):
        return _LOG_OBSERVER
    return _NOOP_OBSERVER


def _observe_node_stream(
    *,
    dag_name: str,
    node_name: str,
    stage: int,
    stream: Iterable[Any] | None,
    observer: ExecutionObserver,
) -> Iterator[Any]:
    def _iter() -> Iterator[Any]:
        item_count = 0
        start_time = time.perf_counter()
        status = "success"
        observer.on_node_start(
            dag_name=dag_name,
            node_name=node_name,
            stage=stage,
        )
        iterator = iter(()) if stream is None else iter(stream)
        try:
            for item in iterator:
                item_count += 1
                yield item
        except Exception:
            status = "error"
            raise
        finally:
            elapsed = time.perf_counter() - start_time
            observer.on_node_end(
                NodeRunEvent(
                    dag_name=dag_name,
                    node_name=node_name,
                    stage=stage,
                    output_items=item_count,
                    elapsed_seconds=elapsed,
                    status=status,
                )
            )

    return _iter()


def _observe_dag_stream(
    *,
    dag: StageDag,
    stream: Iterable[Any] | None,
    observer: ExecutionObserver,
) -> Iterator[Any]:
    def _iter() -> Iterator[Any]:
        start_time = time.perf_counter()
        item_count = 0
        status = "success"
        observer.on_dag_start(dag_name=dag.name, node_count=len(dag.nodes))
        iterator = iter(()) if stream is None else iter(stream)
        try:
            for item in iterator:
                item_count += 1
                yield item
        except Exception:
            status = "error"
            raise
        finally:
            observer.on_dag_end(
                DagRunEvent(
                    dag_name=dag.name,
                    node_count=len(dag.nodes),
                    output_items=item_count,
                    elapsed_seconds=(time.perf_counter() - start_time),
                    status=status,
                )
            )

    return _iter()
