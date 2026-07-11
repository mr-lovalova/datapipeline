import logging
import math
import threading
import time
from dataclasses import dataclass
from collections.abc import Iterable, Iterator
from contextvars import ContextVar, Token, copy_context
from typing import Any

from datapipeline.dag.dag import Dag
from datapipeline.dag.events import (
    DagParentRef,
    DagRunEvent,
    NodeExecutionEvent,
    NodeProgressEvent,
    RunStatus,
)
from datapipeline.dag.observer import (
    ExecutionObserver,
    LoggingExecutionObserver,
    NoopExecutionObserver,
)
from datapipeline.dag.context import PipelineContext
from datapipeline.dag.node import NodeKind, PipelineNode

logger = logging.getLogger(__name__)
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0
_LOG_OBSERVER = LoggingExecutionObserver(logger)
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
_CURRENT_RUN_OBSERVER: ContextVar[ExecutionObserver | None] = ContextVar(
    "datapipeline_runner_observer",
    default=None,
)
_CURRENT_RUN_HEARTBEAT: ContextVar["_RunHeartbeat | None"] = ContextVar(
    "datapipeline_runner_heartbeat",
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
    Token[ExecutionObserver | None],
]


@dataclass
class _HeartbeatNodeState:
    active_since: float
    last_emit_at: float
    items: int
    detail: str | None = None


class _RunHeartbeat:
    def __init__(self, observer: ExecutionObserver, interval_seconds: float) -> None:
        self._observer = observer
        self._interval_seconds = float(interval_seconds)
        self._delivery_lock = threading.RLock()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._stack: list[NodeProgressContext] = []
        self._states: dict[NodeProgressContext, _HeartbeatNodeState] = {}

    def start(self) -> None:
        if self._interval_seconds <= 0:
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

    def enter(self, node: NodeProgressContext, items: int) -> None:
        now = time.perf_counter()
        with self._lock:
            state = self._states.get(node)
            if state is None:
                state = _HeartbeatNodeState(
                    active_since=now,
                    last_emit_at=now,
                    items=items,
                )
                self._states[node] = state
            else:
                state.items = items
            self._stack.append(node)

    def leave(self, node: NodeProgressContext) -> None:
        with self._lock:
            if not self._stack or self._stack[-1] != node:
                raise RuntimeError("Heartbeat node stack is out of order")
            self._stack.pop()

    def item_emitted(self, node: NodeProgressContext, items: int) -> None:
        with self._lock:
            state = self._states.get(node)
            if state is not None:
                state.items = items

    def progress_emitted(self, node: NodeProgressContext) -> None:
        now = time.perf_counter()
        with self._lock:
            state = self._states.get(node)
            if state is not None:
                state.last_emit_at = now

    def set_detail(self, node: NodeProgressContext, detail: str | None) -> None:
        with self._lock:
            state = self._states.get(node)
            if state is not None:
                state.detail = detail

    def clear(self, node: NodeProgressContext) -> None:
        with self._delivery_lock:
            with self._lock:
                self._stack = [item for item in self._stack if item != node]
                self._states.pop(node, None)

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            with self._delivery_lock:
                event = self._snapshot_event()
                if event is None:
                    continue
                node, message = event
                try:
                    _emit_node_progress(self._observer, node, message)
                except Exception:
                    logger.debug("Failed to emit DAG heartbeat", exc_info=True)

    def _snapshot_event(self) -> tuple[NodeProgressContext, str] | None:
        now = time.perf_counter()
        with self._lock:
            if not self._stack:
                return None
            node = self._stack[-1]
            state = self._states.get(node)
            if state is None:
                self._stack.pop()
                return None
            if now - state.last_emit_at < self._interval_seconds:
                return None
            items = state.items
            detail = state.detail
            elapsed = now - state.active_since
            state.last_emit_at = now
        if detail is not None:
            return node, f"running elapsed={elapsed:.0f}s {detail}"
        return node, f"running elapsed={elapsed:.0f}s items={items}"


_DagContextTokens = tuple[
    Token[_RootRunState | None],
    Token[int],
    Token[_RunHeartbeat | None] | None,
]


def _current_run_dag_depth() -> int:
    return max(0, int(_CURRENT_RUN_DAG_DEPTH.get()))


def current_node_progress_context() -> NodeProgressContext | None:
    return _CURRENT_RUN_PROGRESS_NODE.get()


def emit_node_progress(message: str) -> None:
    node = _CURRENT_RUN_PROGRESS_NODE.get()
    observer = _CURRENT_RUN_OBSERVER.get()
    if node is None or observer is None:
        return
    _emit_node_progress(observer, node, message)
    heartbeat = _CURRENT_RUN_HEARTBEAT.get()
    if heartbeat is not None:
        heartbeat.progress_emitted(node)


def set_node_heartbeat_detail(detail: str | None) -> None:
    """Describe buffered work that has not produced node output yet."""
    node = _CURRENT_RUN_PROGRESS_NODE.get()
    heartbeat = _CURRENT_RUN_HEARTBEAT.get()
    if node is not None and heartbeat is not None:
        heartbeat.set_detail(node, detail)


def _emit_node_progress(
    observer: ExecutionObserver,
    node: NodeProgressContext,
    message: str,
) -> None:
    emit = getattr(observer, "on_node_progress", None)
    if not callable(emit):
        return
    emit(
        NodeProgressEvent(
            dag_name=node.dag_name,
            node_name=node.node_name,
            node_index=node.node_index,
            execution_index=node.execution_index,
            node_kind=node.node_kind,
            node_calls_dag=node.node_calls_dag,
            message=message,
            depth=node.depth,
        )
    )


def _close_iterator(iterator: Iterable[Any]) -> None:
    closer = getattr(iterator, "close", None)
    if not callable(closer):
        return
    try:
        closer()
    except Exception:
        logger.debug("Failed to close iterator during DAG teardown", exc_info=True)


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
        raise RuntimeError("Cannot construct a nested DAG outside a root run")

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

    return _observe_dag_stream(
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
    if logger.isEnabledFor(logging.INFO):
        return _LOG_OBSERVER
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
                _CURRENT_RUN_OBSERVER.set(observer),
            )

        def _deactivate_node_context(tokens: _NodeContextTokens) -> None:
            active_node_token, progress_node_token, observer_token = tokens
            _CURRENT_RUN_OBSERVER.reset(observer_token)
            _CURRENT_RUN_PROGRESS_NODE.reset(progress_node_token)
            _CURRENT_RUN_ACTIVE_NODE.reset(active_node_token)

        context_tokens = _activate_node_context()
        heartbeat = _CURRENT_RUN_HEARTBEAT.get()
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
            iterator = iter(stream)
            while True:
                if heartbeat is not None:
                    heartbeat.enter(progress_node, item_count)
                try:
                    item = next(iterator)
                except StopIteration:
                    break
                finally:
                    if heartbeat is not None:
                        heartbeat.leave(progress_node)
                item_count += 1
                if heartbeat is not None:
                    heartbeat.item_emitted(progress_node, item_count)
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
                if heartbeat is not None:
                    heartbeat.clear(progress_node)
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
        heartbeat: _RunHeartbeat | None = None
        dag_started = False

        def activate_dag_context() -> _DagContextTokens:
            return (
                _CURRENT_ROOT_RUN.set(run_state),
                _CURRENT_RUN_DAG_DEPTH.set(dag_depth + 1),
                (
                    _CURRENT_RUN_HEARTBEAT.set(heartbeat)
                    if heartbeat is not None
                    else None
                ),
            )

        def deactivate_dag_context(tokens: _DagContextTokens) -> None:
            root_run_token, depth_token, heartbeat_token = tokens
            if heartbeat_token is not None:
                _CURRENT_RUN_HEARTBEAT.reset(heartbeat_token)
            _CURRENT_RUN_DAG_DEPTH.reset(depth_token)
            _CURRENT_ROOT_RUN.reset(root_run_token)

        context_tokens = activate_dag_context()

        try:
            observer.on_dag_start(
                dag_name=dag.name,
                node_count=dag.node_count,
                depth=dag_depth,
                dag_metadata=dag.metadata,
                dag_parent=dag_parent,
            )
            dag_started = True
            iterator = iter(stream)
            if is_root_run:
                interval_seconds = resolve_heartbeat_interval_seconds(
                    context.heartbeat_interval_seconds
                )
                if interval_seconds > 0:
                    new_heartbeat = _RunHeartbeat(observer, interval_seconds)
                    new_heartbeat.start()
                    heartbeat = new_heartbeat
                    context_tokens = (
                        context_tokens[0],
                        context_tokens[1],
                        _CURRENT_RUN_HEARTBEAT.set(heartbeat),
                    )
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
                    if heartbeat is not None:
                        heartbeat.stop()
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
