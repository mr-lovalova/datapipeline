import logging
import time
from collections.abc import Iterable, Iterator
from contextvars import ContextVar
from typing import Any

from datapipeline.dag.dag import StageDag
from datapipeline.dag.events import DagParentRef, DagRunEvent, StepRunEvent
from datapipeline.dag.observer import (
    ExecutionObserver,
    LoggingExecutionObserver,
    NoopExecutionObserver,
)
from datapipeline.dag.context import PipelineContext
from datapipeline.dag.node import StepKind

logger = logging.getLogger(__name__)
_LOG_OBSERVER = LoggingExecutionObserver(logger)
_NOOP_OBSERVER = NoopExecutionObserver()
_CURRENT_RUN_DAG_DEPTH: ContextVar[int] = ContextVar(
    "datapipeline_runner_current_dag_depth",
    default=0,
)
_CURRENT_RUN_INTERRUPTED: ContextVar[bool] = ContextVar(
    "datapipeline_runner_interrupted",
    default=False,
)
_CURRENT_RUN_ACTIVE_STEP: ContextVar[DagParentRef | None] = ContextVar(
    "datapipeline_runner_active_step",
    default=None,
)


def _current_run_dag_depth() -> int:
    return max(0, int(_CURRENT_RUN_DAG_DEPTH.get()))


def _run_interrupted() -> bool:
    return bool(_CURRENT_RUN_INTERRUPTED.get())


def _current_run_active_step() -> DagParentRef | None:
    return _CURRENT_RUN_ACTIVE_STEP.get()


def _close_iterator(iterator: Iterable[Any]) -> None:
    closer = getattr(iterator, "close", None)
    if not callable(closer):
        return
    try:
        closer()
    except Exception:
        logger.debug("Failed to close iterator during DAG teardown", exc_info=True)


def run_stage_dag(
    context: PipelineContext,
    dag: StageDag,
    *,
    seed: Iterable[Any] | None = None,
    observer: ExecutionObserver | None = None,
) -> Iterator[Any]:
    active_observer = _resolve_observer(context, observer)
    dag_depth = _current_run_dag_depth()
    is_root_run = dag_depth == 0
    construction_token = _CURRENT_RUN_DAG_DEPTH.set(dag_depth + 1)
    if is_root_run:
        _CURRENT_RUN_ACTIVE_STEP.set(None)
    stream: Iterable[Any] | None = seed
    try:
        state: dict[str, Iterable[Any] | None] = {}
        if seed is not None:
            state["seed"] = seed
        for index, step in enumerate(dag.steps):
            if step.input is None:
                step_input = stream
            else:
                if step.input not in state:
                    available = ", ".join(sorted(state.keys())) or "(none)"
                    raise KeyError(
                        f"Step '{step.name}' requested missing input "
                        f"'{step.input}' in DAG '{dag.name}'. "
                        f"Available outputs: {available}"
                    )
                step_input = state[step.input]
            produced = _run_step(
                step,
                step_input,
                include_input=(step.input is not None),
            )
            stream = _observe_step_stream(
                dag_name=dag.name,
                step_name=step.name,
                step_index=index,
                step_kind=step.kind,
                step_calls_dag=step.calls_dag,
                depth=dag_depth + 1,
                stream=produced,
                observer=active_observer,
            )
            state[step.output or step.name] = stream
        return _observe_dag_stream(
            dag=dag,
            depth=dag_depth,
            reset_interrupt=is_root_run,
            stream=stream,
            observer=active_observer,
        )
    finally:
        _CURRENT_RUN_DAG_DEPTH.reset(construction_token)
        if is_root_run:
            _CURRENT_RUN_ACTIVE_STEP.set(None)


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


def _run_step(
    step,
    step_input: Iterable[Any] | None,
    *,
    include_input: bool,
) -> Iterable[Any] | None:
    kwargs = dict(step.kwargs or {})
    if include_input:
        return step.op(*step.args, step_input, **kwargs)
    return step.op(*step.args, **kwargs)


def _observe_step_stream(
    *,
    dag_name: str,
    step_name: str,
    step_index: int,
    step_kind: StepKind,
    step_calls_dag: str | None,
    depth: int,
    stream: Iterable[Any] | None,
    observer: ExecutionObserver,
) -> Iterator[Any]:
    def _iter() -> Iterator[Any]:
        step_depth = max(0, int(depth))
        item_count = 0
        start_time = time.perf_counter()
        status = "success"
        error_type: str | None = None
        iterator: Iterator[Any] = iter(())
        active_step_token = _CURRENT_RUN_ACTIVE_STEP.set(
            DagParentRef(dag_name=dag_name, step_name=step_name, step_index=step_index)
        )
        try:
            observer.on_step_start(
                dag_name=dag_name,
                step_name=step_name,
                step_index=step_index,
                step_kind=step_kind,
                step_calls_dag=step_calls_dag,
                depth=step_depth,
            )
            iterator = iter(()) if stream is None else iter(stream)
            for item in iterator:
                item_count += 1
                yield item
        except KeyboardInterrupt as exc:
            status = "error"
            error_type = type(exc).__name__
            _CURRENT_RUN_INTERRUPTED.set(True)
            raise
        except Exception as exc:
            status = "error"
            error_type = type(exc).__name__
            raise
        finally:
            try:
                _close_iterator(iterator)
                if status == "success" and _run_interrupted():
                    status = "error"
                    error_type = "KeyboardInterrupt"
                elapsed = time.perf_counter() - start_time
                observer.on_step_end(
                    StepRunEvent(
                        dag_name=dag_name,
                        step_name=step_name,
                        step_index=step_index,
                        output_items=item_count,
                        elapsed_seconds=elapsed,
                        status=status,
                        step_kind=step_kind,
                        step_calls_dag=step_calls_dag,
                        error_type=error_type,
                        depth=step_depth,
                    )
                )
            finally:
                _CURRENT_RUN_ACTIVE_STEP.reset(active_step_token)

    return _iter()


def _observe_dag_stream(
    *,
    dag: StageDag,
    depth: int,
    reset_interrupt: bool,
    stream: Iterable[Any] | None,
    observer: ExecutionObserver,
) -> Iterator[Any]:
    def _iter() -> Iterator[Any]:
        dag_depth = max(0, int(depth))
        dag_parent = _current_run_active_step()
        if reset_interrupt:
            _CURRENT_RUN_INTERRUPTED.set(False)
        depth_token = _CURRENT_RUN_DAG_DEPTH.set(dag_depth + 1)
        start_time = time.perf_counter()
        item_count = 0
        status = "success"
        error_type: str | None = None
        observer.on_dag_start(
            dag_name=dag.name,
            step_count=len(dag.nodes),
            depth=dag_depth,
            dag_metadata=dag.metadata,
            dag_parent=dag_parent,
        )
        iterator = iter(()) if stream is None else iter(stream)
        try:
            for item in iterator:
                item_count += 1
                yield item
        except KeyboardInterrupt as exc:
            status = "error"
            error_type = type(exc).__name__
            _CURRENT_RUN_INTERRUPTED.set(True)
            raise
        except Exception as exc:
            status = "error"
            error_type = type(exc).__name__
            raise
        finally:
            _close_iterator(iterator)
            if status == "success" and _run_interrupted():
                status = "error"
                error_type = "KeyboardInterrupt"
            try:
                observer.on_dag_end(
                    DagRunEvent(
                        dag_name=dag.name,
                        step_count=len(dag.nodes),
                        output_items=item_count,
                        elapsed_seconds=(time.perf_counter() - start_time),
                        status=status,
                        error_type=error_type,
                        depth=dag_depth,
                        parent=dag_parent,
                    )
                )
            finally:
                _CURRENT_RUN_DAG_DEPTH.reset(depth_token)

    return _iter()
