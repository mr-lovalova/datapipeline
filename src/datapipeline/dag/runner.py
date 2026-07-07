import logging
import threading
import time
from dataclasses import dataclass
from collections.abc import Iterable, Iterator
from contextvars import ContextVar, copy_context
from typing import Any

from datapipeline.dag.dag import Dag
from datapipeline.dag.events import (
    DagParentRef,
    DagRunEvent,
    NodeExecutionEvent,
    NodeProgressEvent,
)
from datapipeline.dag.observer import (
    ExecutionObserver,
    LoggingExecutionObserver,
    NoopExecutionObserver,
)
from datapipeline.dag.context import PipelineContext
from datapipeline.dag.node import NodeKind

logger = logging.getLogger(__name__)
_HEARTBEAT_INTERVAL_SECONDS = 60.0
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
_CURRENT_RUN_EXECUTION_INDEX: ContextVar[int] = ContextVar(
    "datapipeline_runner_execution_index",
    default=0,
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


@dataclass
class _HeartbeatNodeState:
    active_since: float
    last_emit_at: float
    items: int


class _RunHeartbeat:
    def __init__(self, observer: ExecutionObserver, interval_seconds: float) -> None:
        self._observer = observer
        self._interval_seconds = float(interval_seconds)
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

    def clear(self, node: NodeProgressContext) -> None:
        with self._lock:
            self._stack = [item for item in self._stack if item != node]
            self._states.pop(node, None)

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
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
            elapsed = now - state.active_since
            state.last_emit_at = now
        return node, f"running elapsed={elapsed:.0f}s items={items}"


def _current_run_dag_depth() -> int:
    return max(0, int(_CURRENT_RUN_DAG_DEPTH.get()))


def _run_interrupted() -> bool:
    return bool(_CURRENT_RUN_INTERRUPTED.get())


def _current_run_active_node() -> DagParentRef | None:
    return _CURRENT_RUN_ACTIVE_NODE.get()


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


def _next_execution_index() -> int:
    current = max(0, int(_CURRENT_RUN_EXECUTION_INDEX.get()))
    _CURRENT_RUN_EXECUTION_INDEX.set(current + 1)
    return current


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


def _heartbeat_interval_seconds(context: PipelineContext) -> float:
    interval = getattr(context, "heartbeat_interval_seconds", None)
    if interval is None:
        return _HEARTBEAT_INTERVAL_SECONDS
    interval = float(interval)
    if interval < 0:
        raise ValueError("heartbeat_interval_seconds must be non-negative")
    return interval


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
    construction_token = _CURRENT_RUN_DAG_DEPTH.set(dag_depth + 1)
    if is_root_run:
        _CURRENT_RUN_ACTIVE_NODE.set(None)
        _CURRENT_RUN_EXECUTION_INDEX.set(0)
    stream: Iterable[Any] | None = seed
    try:
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
            produced = _run_node(
                node,
                node_input,
                state,
                include_input=(node.input is not None),
            )
            stream = _observe_node_stream(
                dag_name=dag.name,
                node_name=node.name,
                node_index=index,
                node_kind=node.kind,
                node_calls_dag=node.calls_dag,
                depth=dag_depth + 1,
                stream=produced,
                observer=active_observer,
            )
            state[node.output or node.name] = stream
        return _observe_dag_stream(
            context=context,
            dag=dag,
            depth=dag_depth,
            reset_interrupt=is_root_run,
            stream=stream,
            observer=active_observer,
        )
    finally:
        _CURRENT_RUN_DAG_DEPTH.reset(construction_token)
        if is_root_run:
            _CURRENT_RUN_ACTIVE_NODE.set(None)


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
    state: dict[str, Iterable[Any] | None],
    *,
    include_input: bool,
) -> Iterable[Any] | None:
    kwargs = dict(node.kwargs or {})
    for kwarg_name, state_key in dict(node.kwinputs or {}).items():
        if state_key not in state:
            available = ", ".join(sorted(state.keys())) or "(none)"
            raise KeyError(
                f"Node '{node.name}' requested missing kwinput "
                f"'{state_key}'. Available outputs: {available}"
            )
        kwargs[kwarg_name] = state[state_key]
    if include_input:
        return node.op(*node.args, node_input, **kwargs)
    return node.op(*node.args, **kwargs)


def _observe_node_stream(
    *,
    dag_name: str,
    node_name: str,
    node_index: int,
    node_kind: NodeKind,
    node_calls_dag: str | None,
    depth: int,
    stream: Iterable[Any] | None,
    observer: ExecutionObserver,
) -> Iterator[Any]:
    def _iter() -> Iterator[Any]:
        node_depth = max(0, int(depth), _current_run_dag_depth())
        item_count = 0
        start_time = time.perf_counter()
        status = "success"
        error_type: str | None = None
        error_message: str | None = None
        iterator: Iterator[Any] = iter(())
        execution_index = _next_execution_index()
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

        def _activate_node_context() -> tuple[Any, Any, Any]:
            return (
                _CURRENT_RUN_ACTIVE_NODE.set(active_node),
                _CURRENT_RUN_PROGRESS_NODE.set(progress_node),
                _CURRENT_RUN_OBSERVER.set(observer),
            )

        def _deactivate_node_context(tokens: tuple[Any, Any, Any]) -> None:
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
            iterator = iter(()) if stream is None else iter(stream)
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
                context_tokens = None
                try:
                    yield item
                finally:
                    context_tokens = _activate_node_context()
        except KeyboardInterrupt as exc:
            status = "error"
            error_type = type(exc).__name__
            error_message = _error_message(exc)
            _CURRENT_RUN_INTERRUPTED.set(True)
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
                if status == "success" and _run_interrupted():
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
                if context_tokens is not None:
                    _deactivate_node_context(context_tokens)

    return _iter()


def _observe_dag_stream(
    *,
    context: PipelineContext,
    dag: Dag,
    depth: int,
    reset_interrupt: bool,
    stream: Iterable[Any] | None,
    observer: ExecutionObserver,
) -> Iterator[Any]:
    def _iter() -> Iterator[Any]:
        dag_depth = max(0, int(depth))
        dag_parent = _current_run_active_node()
        parent_node = current_node_progress_context()
        if dag_parent is not None and parent_node is not None:
            dag_depth = max(dag_depth, parent_node.depth)
        if reset_interrupt:
            _CURRENT_RUN_INTERRUPTED.set(False)
        depth_token = _CURRENT_RUN_DAG_DEPTH.set(dag_depth + 1)
        start_time = time.perf_counter()
        item_count = 0
        status = "success"
        error_type: str | None = None
        error_message: str | None = None
        observer.on_dag_start(
            dag_name=dag.name,
            node_count=dag.node_count,
            depth=dag_depth,
            dag_metadata=dag.metadata,
            dag_parent=dag_parent,
        )
        iterator = iter(()) if stream is None else iter(stream)
        heartbeat: _RunHeartbeat | None = None
        heartbeat_token = None
        if reset_interrupt:
            interval_seconds = _heartbeat_interval_seconds(context)
            if interval_seconds > 0:
                heartbeat = _RunHeartbeat(observer, interval_seconds)
                heartbeat.start()
                heartbeat_token = _CURRENT_RUN_HEARTBEAT.set(heartbeat)
        try:
            for item in iterator:
                item_count += 1
                yield item
        except KeyboardInterrupt as exc:
            status = "error"
            error_type = type(exc).__name__
            error_message = _error_message(exc)
            _CURRENT_RUN_INTERRUPTED.set(True)
            raise
        except Exception as exc:
            status = "error"
            error_type = type(exc).__name__
            error_message = _error_message(exc)
            raise
        finally:
            try:
                _close_iterator(iterator)
                if status == "success" and _run_interrupted():
                    status = "error"
                    error_type = "KeyboardInterrupt"
                try:
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
                    if heartbeat is not None:
                        heartbeat.stop()
                    if heartbeat_token is not None:
                        _CURRENT_RUN_HEARTBEAT.reset(heartbeat_token)
            finally:
                _CURRENT_RUN_DAG_DEPTH.reset(depth_token)

    return _iter()
