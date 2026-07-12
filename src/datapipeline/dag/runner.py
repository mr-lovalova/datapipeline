import math
import threading
import time
from dataclasses import dataclass, replace
from collections.abc import Iterable, Iterator
from contextvars import ContextVar, Token, copy_context
from typing import Any

from datapipeline.dag.dag import Dag
from datapipeline.dag.events import (
    DagParentRef,
    DagRunEvent,
    NodeExecutionEvent,
    NodeProgressEvent,
    ProgressSnapshot,
    RunStatus,
)
from datapipeline.dag.observer import ExecutionObserver, NoopExecutionObserver
from datapipeline.dag.context import PipelineContext
from datapipeline.dag.node import NodeKind, PipelineNode

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0
_LIVE_PROGRESS_INTERVAL_SECONDS = 0.1
_NOOP_OBSERVER = NoopExecutionObserver()


@dataclass
class _RootRunState:
    execution_index: int = 0
    interrupted: bool = False


_CURRENT_RUN_DAG_DEPTH: ContextVar[int] = ContextVar(
    "datapipeline_runner_current_dag_depth",
    default=0,
)
_CURRENT_ROOT_RUN: ContextVar[_RootRunState | None] = ContextVar(
    "datapipeline_runner_current_root_run",
    default=None,
)
_CURRENT_RUN_ACTIVE_NODE: ContextVar[DagParentRef | None] = ContextVar(
    "datapipeline_runner_active_node",
    default=None,
)
_CURRENT_RUN_PROGRESS_NODE: ContextVar["NodeProgressContext | None"] = ContextVar(
    "datapipeline_runner_progress_node",
    default=None,
)
_CURRENT_RUN_PROGRESS: ContextVar["_RunProgress | None"] = ContextVar(
    "datapipeline_runner_progress",
    default=None,
)


@dataclass(frozen=True)
class NodeProgressContext:
    dag_name: str
    node_name: str
    node_index: int
    execution_index: int
    node_kind: NodeKind
    node_calls_dag: str | None
    depth: int


_NodeContextTokens = tuple[
    Token[DagParentRef | None],
    Token[NodeProgressContext | None],
]


@dataclass
class _NodeProgressState:
    active_since: float
    last_live_at: float
    last_persistent_at: float
    completed: int
    progress: ProgressSnapshot


class _RunProgress:
    def __init__(self, observer: ExecutionObserver, interval_seconds: float) -> None:
        self._observer = observer
        self._heartbeat_interval_seconds = float(interval_seconds)
        self._delivery_lock = threading.RLock()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._thread_error: Exception | None = None
        self._stack: list[NodeProgressContext] = []
        self._states: dict[NodeProgressContext, _NodeProgressState] = {}

    def start(self) -> None:
        if self._heartbeat_interval_seconds <= 0:
            return
        context = copy_context()
        self._thread = threading.Thread(
            target=lambda: context.run(self._run),
            name="datapipeline-dag-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            if self._thread.is_alive():
                raise RuntimeError("DAG heartbeat thread did not stop")
        if self._thread_error is not None:
            raise RuntimeError("DAG heartbeat observer failed") from self._thread_error

    def start_node(self, node: NodeProgressContext) -> None:
        now = time.perf_counter()
        with self._lock:
            self._states[node] = _NodeProgressState(
                active_since=now,
                last_live_at=now,
                last_persistent_at=now,
                completed=0,
                progress=ProgressSnapshot(completed=0, unit="out"),
            )

    def enter(self, node: NodeProgressContext) -> None:
        if self._heartbeat_interval_seconds <= 0:
            return
        with self._lock:
            self._stack.append(node)

    def leave(self, node: NodeProgressContext) -> None:
        if self._heartbeat_interval_seconds <= 0:
            return
        with self._lock:
            if not self._stack or self._stack[-1] != node:
                raise RuntimeError("Progress node stack is out of order")
            self._stack.pop()

    def item_emitted(self, node: NodeProgressContext, items: int) -> None:
        should_emit = False
        now = time.perf_counter()
        with self._lock:
            state = self._states.get(node)
            if state is None:
                return
            state.completed = items
            if (
                items == 1
                or now - state.last_live_at >= _LIVE_PROGRESS_INTERVAL_SECONDS
            ):
                state.last_live_at = now
                should_emit = True
        if should_emit:
            self._emit(node, persistent=False)

    def report(self, node: NodeProgressContext, progress: ProgressSnapshot) -> None:
        should_emit = False
        now = time.perf_counter()
        with self._lock:
            state = self._states.get(node)
            if state is None:
                return
            previous = state.progress
            state.completed = progress.completed
            state.progress = progress
            presentation_changed = (
                progress.phase != previous.phase
                or progress.total != previous.total
                or progress.unit != previous.unit
                or progress.resource != previous.resource
            )
            if (
                presentation_changed
                or now - state.last_live_at >= _LIVE_PROGRESS_INTERVAL_SECONDS
            ):
                state.last_live_at = now
                should_emit = True
        if should_emit:
            self._emit(node, persistent=False)

    def clear(self, node: NodeProgressContext) -> None:
        with self._delivery_lock:
            with self._lock:
                self._stack = [item for item in self._stack if item != node]
                self._states.pop(node, None)

    def _run(self) -> None:
        try:
            while not self._stop.wait(self._heartbeat_interval_seconds):
                self._emit_heartbeat()
        except Exception as exc:
            self._thread_error = exc
            self._stop.set()

    def _emit_heartbeat(self) -> None:
        now = time.perf_counter()
        with self._lock:
            if not self._stack:
                return
            node = self._stack[-1]
            state = self._states.get(node)
            if state is None:
                self._stack.pop()
                return
            if now - state.last_persistent_at < self._heartbeat_interval_seconds:
                return
            state.last_persistent_at = now
        self._emit(node, persistent=True)

    def _emit(self, node: NodeProgressContext, persistent: bool) -> None:
        with self._delivery_lock:
            now = time.perf_counter()
            with self._lock:
                state = self._states.get(node)
                if state is None:
                    return
                progress = state.progress
                if state.completed != progress.completed:
                    progress = replace(progress, completed=state.completed)
                elapsed = now - state.active_since
            _emit_node_progress(
                self._observer,
                node,
                progress,
                elapsed_seconds=elapsed,
                persistent=persistent,
            )


_DagContextTokens = tuple[
    Token[_RootRunState | None],
    Token[int],
    Token[_RunProgress | None],
]


def _current_run_dag_depth() -> int:
    return max(0, int(_CURRENT_RUN_DAG_DEPTH.get()))


def current_node_progress_context() -> NodeProgressContext | None:
    return _CURRENT_RUN_PROGRESS_NODE.get()


def report_node_progress(progress: ProgressSnapshot) -> None:
    node = _CURRENT_RUN_PROGRESS_NODE.get()
    publisher = _CURRENT_RUN_PROGRESS.get()
    if node is None or publisher is None:
        return
    publisher.report(node, progress)


def _emit_node_progress(
    observer: ExecutionObserver,
    node: NodeProgressContext,
    progress: ProgressSnapshot,
    elapsed_seconds: float,
    persistent: bool,
) -> None:
    observer.on_node_progress(
        NodeProgressEvent(
            dag_name=node.dag_name,
            node_name=node.node_name,
            node_index=node.node_index,
            execution_index=node.execution_index,
            node_kind=node.node_kind,
            node_calls_dag=node.node_calls_dag,
            progress=progress,
            elapsed_seconds=elapsed_seconds,
            persistent=persistent,
            depth=node.depth,
        )
    )


def _close_iterator(iterator: Iterable[Any]) -> None:
    closer = getattr(iterator, "close", None)
    if callable(closer):
        closer()


def _error_message(exc: BaseException) -> str | None:
    message = str(exc).strip()
    return message or None


def resolve_heartbeat_interval_seconds(interval: float | None) -> float:
    if interval is None:
        return DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    interval = float(interval)
    if not math.isfinite(interval):
        raise ValueError("heartbeat_interval_seconds must be finite")
    if interval < 0:
        raise ValueError("heartbeat_interval_seconds must be non-negative")
    if interval > threading.TIMEOUT_MAX:
        raise ValueError(
            f"heartbeat_interval_seconds must not exceed {threading.TIMEOUT_MAX:g}"
        )
    return interval


def _validate_stream_bindings(
    dag: Dag,
    node_bindings: tuple[tuple[PipelineNode, dict[str, str]], ...],
    seed_available: bool,
) -> None:
    producer_by_output: dict[str, int | None] = {"seed": None} if seed_available else {}
    consumer_by_producer: dict[int | None, str] = {}

    for node_index, (node, kwinputs) in enumerate(node_bindings):
        input_keys: list[str] = []
        if node.input is not None:
            input_keys.append(node.input)
        input_keys.extend(kwinputs.values())

        for state_key in input_keys:
            if state_key not in producer_by_output:
                available = ", ".join(sorted(producer_by_output)) or "(none)"
                raise KeyError(
                    f"Node '{node.name}' requested missing input "
                    f"'{state_key}' in DAG '{dag.name}'. "
                    f"Available outputs: {available}"
                )
            producer = producer_by_output[state_key]
            if producer in consumer_by_producer:
                previous_consumer = consumer_by_producer[producer]
                raise ValueError(
                    f"Stream '{state_key}' in DAG '{dag.name}' is already consumed "
                    f"by node '{previous_consumer}'; node '{node.name}' cannot "
                    "consume it again."
                )
            consumer_by_producer[producer] = node.name

        producer_by_output[node.output or node.name] = node_index


def run_dag(
    context: PipelineContext,
    dag: Dag,
    *,
    seed: Iterable[Any] | None = None,
    observer: ExecutionObserver | None = None,
) -> Iterator[Any]:
    active_observer = _resolve_observer(context, observer)
    dag_depth = _current_run_dag_depth()
    is_root_run = dag_depth == 0
    run_state = _RootRunState() if is_root_run else _CURRENT_ROOT_RUN.get()
    if run_state is None:
        raise RuntimeError("Cannot execute a nested DAG outside a root run")

    def _build_stream() -> Iterator[Any]:
        node_bindings = tuple((node, dict(node.kwinputs or {})) for node in dag.nodes)
        _validate_stream_bindings(dag, node_bindings, seed is not None)
        stream: Iterable[Any] = () if seed is None else seed
        state: dict[str, Iterable[Any]] = {} if seed is None else {"seed": seed}
        for index, (node, kwinputs) in enumerate(node_bindings):
            kwargs = dict(node.kwargs or {})
            for kwarg_name, state_key in kwinputs.items():
                kwargs[kwarg_name] = state[state_key]
            if node.input is None:
                produced = node.op(*node.args, **kwargs)
            else:
                produced = node.op(*node.args, state[node.input], **kwargs)
            if produced is None:
                raise TypeError(
                    f"Node '{node.name}' in DAG '{dag.name}' returned None; "
                    "node operations must return an iterable. "
                    "Return () for an empty stream."
                )
            stream = _observe_node_stream(
                dag_name=dag.name,
                node_name=node.name,
                node_index=index,
                node_kind=node.kind,
                node_calls_dag=node.calls_dag,
                depth=dag_depth + 1,
                run_state=run_state,
                stream=produced,
                observer=active_observer,
            )
            state[node.output or node.name] = stream
        yield from stream

    yield from _observe_dag_stream(
        context=context,
        dag=dag,
        depth=dag_depth,
        is_root_run=is_root_run,
        run_state=run_state,
        stream=_build_stream(),
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
    return _NOOP_OBSERVER


def _observe_node_stream(
    *,
    dag_name: str,
    node_name: str,
    node_index: int,
    node_kind: NodeKind,
    node_calls_dag: str | None,
    depth: int,
    run_state: _RootRunState,
    stream: Iterable[Any],
    observer: ExecutionObserver,
) -> Iterator[Any]:
    def _iter() -> Iterator[Any]:
        node_depth = max(0, int(depth), _current_run_dag_depth())
        item_count = 0
        start_time = time.perf_counter()
        status: RunStatus = "success"
        error_type: str | None = None
        error_message: str | None = None
        iterator: Iterator[Any] = iter(())
        execution_index = run_state.execution_index
        run_state.execution_index += 1
        progress_node = NodeProgressContext(
            dag_name=dag_name,
            node_name=node_name,
            node_index=node_index,
            execution_index=execution_index,
            node_kind=node_kind,
            node_calls_dag=node_calls_dag,
            depth=node_depth,
        )
        active_node = DagParentRef(
            dag_name=dag_name,
            node_name=node_name,
            node_index=node_index,
        )

        def _activate_node_context() -> _NodeContextTokens:
            return (
                _CURRENT_RUN_ACTIVE_NODE.set(active_node),
                _CURRENT_RUN_PROGRESS_NODE.set(progress_node),
            )

        def _deactivate_node_context(tokens: _NodeContextTokens) -> None:
            active_node_token, progress_node_token = tokens
            _CURRENT_RUN_PROGRESS_NODE.reset(progress_node_token)
            _CURRENT_RUN_ACTIVE_NODE.reset(active_node_token)

        progress = _CURRENT_RUN_PROGRESS.get()
        if progress is None:
            raise RuntimeError("Cannot execute a DAG node outside a root run")
        context_tokens = _activate_node_context()
        try:
            observer.on_node_start(
                dag_name=dag_name,
                node_name=node_name,
                node_index=node_index,
                execution_index=execution_index,
                node_kind=node_kind,
                node_calls_dag=node_calls_dag,
                depth=node_depth,
            )
            progress.start_node(progress_node)
            iterator = iter(stream)
            while True:
                progress.enter(progress_node)
                try:
                    item = next(iterator)
                except StopIteration:
                    break
                finally:
                    progress.leave(progress_node)
                item_count += 1
                progress.item_emitted(progress_node, item_count)
                _deactivate_node_context(context_tokens)
                try:
                    yield item
                finally:
                    context_tokens = _activate_node_context()
        except KeyboardInterrupt as exc:
            status = "error"
            error_type = type(exc).__name__
            error_message = _error_message(exc)
            run_state.interrupted = True
            raise
        except Exception as exc:
            status = "error"
            error_type = type(exc).__name__
            error_message = _error_message(exc)
            raise
        finally:
            try:
                progress.clear(progress_node)
                _close_iterator(iterator)
                if status == "success" and run_state.interrupted:
                    status = "error"
                    error_type = "KeyboardInterrupt"
                elapsed = time.perf_counter() - start_time
                observer.on_node_end(
                    NodeExecutionEvent(
                        dag_name=dag_name,
                        node_name=node_name,
                        node_index=node_index,
                        execution_index=execution_index,
                        output_items=item_count,
                        elapsed_seconds=elapsed,
                        status=status,
                        node_kind=node_kind,
                        node_calls_dag=node_calls_dag,
                        error_type=error_type,
                        error_message=error_message,
                        depth=node_depth,
                    )
                )
            finally:
                _deactivate_node_context(context_tokens)

    return _iter()


def _observe_dag_stream(
    *,
    context: PipelineContext,
    dag: Dag,
    depth: int,
    is_root_run: bool,
    run_state: _RootRunState,
    stream: Iterable[Any],
    observer: ExecutionObserver,
) -> Iterator[Any]:
    def _iter() -> Iterator[Any]:
        dag_depth = max(0, int(depth))
        dag_parent = _CURRENT_RUN_ACTIVE_NODE.get()
        parent_node = current_node_progress_context()
        if dag_parent is not None and parent_node is not None:
            dag_depth = max(dag_depth, parent_node.depth)
        start_time = time.perf_counter()
        item_count = 0
        status: RunStatus = "success"
        error_type: str | None = None
        error_message: str | None = None
        iterator: Iterator[Any] = iter(())
        progress: _RunProgress
        if is_root_run:
            progress = _RunProgress(
                observer,
                resolve_heartbeat_interval_seconds(context.heartbeat_interval_seconds),
            )
        else:
            inherited_progress = _CURRENT_RUN_PROGRESS.get()
            if inherited_progress is None:
                raise RuntimeError("Cannot execute a nested DAG outside a root run")
            progress = inherited_progress
        dag_started = False

        def activate_dag_context() -> _DagContextTokens:
            return (
                _CURRENT_ROOT_RUN.set(run_state),
                _CURRENT_RUN_DAG_DEPTH.set(dag_depth + 1),
                _CURRENT_RUN_PROGRESS.set(progress),
            )

        def deactivate_dag_context(tokens: _DagContextTokens) -> None:
            root_run_token, depth_token, progress_token = tokens
            _CURRENT_RUN_PROGRESS.reset(progress_token)
            _CURRENT_RUN_DAG_DEPTH.reset(depth_token)
            _CURRENT_ROOT_RUN.reset(root_run_token)

        context_tokens = activate_dag_context()

        try:
            observer.on_dag_start(
                dag_name=dag.name,
                node_count=dag.node_count,
                depth=dag_depth,
                summary=dag.summary,
                dag_parent=dag_parent,
            )
            dag_started = True
            if is_root_run:
                progress.start()
            iterator = iter(stream)
            for item in iterator:
                item_count += 1
                deactivate_dag_context(context_tokens)
                try:
                    yield item
                finally:
                    context_tokens = activate_dag_context()
        except KeyboardInterrupt as exc:
            status = "error"
            error_type = type(exc).__name__
            error_message = _error_message(exc)
            run_state.interrupted = True
            raise
        except Exception as exc:
            status = "error"
            error_type = type(exc).__name__
            error_message = _error_message(exc)
            raise
        finally:
            try:
                try:
                    _close_iterator(iterator)
                finally:
                    if is_root_run:
                        progress.stop()
                if status == "success" and run_state.interrupted:
                    status = "error"
                    error_type = "KeyboardInterrupt"
                if dag_started:
                    observer.on_dag_end(
                        DagRunEvent(
                            dag_name=dag.name,
                            node_count=dag.node_count,
                            output_items=item_count,
                            elapsed_seconds=(time.perf_counter() - start_time),
                            status=status,
                            error_type=error_type,
                            error_message=error_message,
                            depth=dag_depth,
                            parent=dag_parent,
                        )
                    )
            finally:
                deactivate_dag_context(context_tokens)

    return _iter()
