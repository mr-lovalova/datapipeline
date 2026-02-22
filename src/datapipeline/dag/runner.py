import logging
import time
from collections.abc import Iterable, Iterator
from typing import Any

from datapipeline.dag.dag import StageDag
from datapipeline.dag.events import DagRunEvent, NodeRunEvent
from datapipeline.dag.observer import (
    ExecutionObserver,
    LoggingExecutionObserver,
    NoopExecutionObserver,
)
from datapipeline.dag.context import PipelineContext

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
    state: dict[str, Iterable[Any] | None] = {}
    if seed is not None:
        state["seed"] = seed
    for index, node in enumerate(dag.nodes):
        if node.input is None:
            node_input = stream
        else:
            if node.input not in state:
                available = ", ".join(sorted(state.keys())) or "(none)"
                raise KeyError(
                    f"Node '{node.name}' requested missing input "
                    f"'{node.input}' in DAG '{dag.name}'. "
                    f"Available outputs: {available}"
                )
            node_input = state[node.input]
        produced = _run_node(node, node_input, include_input=(node.input is not None))
        stream = _observe_node_stream(
            dag_name=dag.name,
            node_name=node.name,
            stage=index,
            stream=produced,
            observer=active_observer,
        )
        state[node.output or node.name] = stream
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
    if logger.isEnabledFor(logging.INFO):
        return _LOG_OBSERVER
    return _NOOP_OBSERVER


def _run_node(
    node,
    node_input: Iterable[Any] | None,
    *,
    include_input: bool,
) -> Iterable[Any] | None:
    kwargs = dict(node.kwargs or {})
    if include_input:
        return node.op(*node.args, node_input, **kwargs)
    return node.op(*node.args, **kwargs)


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
        error_type: str | None = None
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
        except KeyboardInterrupt as exc:
            status = "error"
            error_type = type(exc).__name__
            raise
        except Exception as exc:
            status = "error"
            error_type = type(exc).__name__
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
                    error_type=error_type,
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
        error_type: str | None = None
        observer.on_dag_start(
            dag_name=dag.name,
            node_count=len(dag.nodes),
            dag_metadata=dag.metadata,
        )
        iterator = iter(()) if stream is None else iter(stream)
        try:
            for item in iterator:
                item_count += 1
                yield item
        except KeyboardInterrupt as exc:
            status = "error"
            error_type = type(exc).__name__
            raise
        except Exception as exc:
            status = "error"
            error_type = type(exc).__name__
            raise
        finally:
            observer.on_dag_end(
                DagRunEvent(
                    dag_name=dag.name,
                    node_count=len(dag.nodes),
                    output_items=item_count,
                    elapsed_seconds=(time.perf_counter() - start_time),
                    status=status,
                    error_type=error_type,
                )
            )

    return _iter()
