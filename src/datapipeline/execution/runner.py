import math
import threading
import time
from collections.abc import Generator, Iterable, Iterator
from contextvars import copy_context
from dataclasses import dataclass
from typing import Any

from datapipeline.execution.context import PipelineContext
from datapipeline.execution.events import (
    NodeExecutionEvent,
    NodeProgressEvent,
    PipelineRunEvent,
    ProgressSnapshot,
    RunStatus,
)
from datapipeline.execution.node import Node, NodeProgressReader, SourceNode
from datapipeline.execution.observer import PipelineObserver, NoopPipelineObserver
from datapipeline.execution.pipeline import Pipeline


DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 60.0
_LIVE_PROGRESS_INTERVAL_SECONDS = 0.1
_NOOP_OBSERVER = NoopPipelineObserver()


@dataclass(frozen=True)
class _NodeProgressContext:
    pipeline_name: str
    node_name: str
    node_index: int


@dataclass
class _NodeProgressState:
    started_at: float
    last_persistent_at: float
    completed: int
    reader: NodeProgressReader | None
    last_live_signature: tuple[object, ...] | None = None


class _RunProgress:
    """Publish sampled progress without synchronizing every emitted item."""

    def __init__(self, observer: PipelineObserver, interval_seconds: float) -> None:
        self._observer = observer
        self._heartbeat_interval_seconds = interval_seconds
        self._delivery_lock = threading.RLock()
        self._state_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._thread_error: Exception | None = None
        self._states: dict[_NodeProgressContext, _NodeProgressState] = {}
        self.active_node: _NodeProgressContext | None = None

    def start(self) -> None:
        context = copy_context()
        self._thread = threading.Thread(
            target=lambda: context.run(self._run),
            name="datapipeline-pipeline-progress",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            if self._thread.is_alive():
                raise RuntimeError("Pipeline progress thread did not stop")
        if self._thread_error is not None:
            raise RuntimeError("Pipeline progress failed") from self._thread_error

    def start_node(
        self,
        node: _NodeProgressContext,
        reader: NodeProgressReader | None,
    ) -> _NodeProgressState:
        now = time.perf_counter()
        state = _NodeProgressState(
            started_at=now,
            last_persistent_at=now,
            completed=0,
            reader=reader,
        )
        with self._state_lock:
            self._states[node] = state
        return state

    def clear(self, node: _NodeProgressContext) -> None:
        with self._delivery_lock:
            with self._state_lock:
                self._states.pop(node, None)
            if self.active_node == node:
                self.active_node = None

    def _run(self) -> None:
        try:
            while not self._stop.wait(_LIVE_PROGRESS_INTERVAL_SECONDS):
                self._emit_due_progress()
        except Exception as exc:
            self._thread_error = exc
            self._stop.set()

    def _emit_due_progress(self) -> None:
        active_node = self.active_node
        now = time.perf_counter()
        with self._delivery_lock:
            due = []
            with self._state_lock:
                for node, state in self._states.items():
                    if node != active_node and state.reader is None:
                        continue
                    progress = self._snapshot(state)
                    signature = self._signature(progress)
                    persistent = (
                        node == active_node
                        and self._heartbeat_interval_seconds > 0
                        and now - state.last_persistent_at
                        >= self._heartbeat_interval_seconds
                    )
                    if persistent:
                        state.last_persistent_at = now
                        state.last_live_signature = signature
                    elif signature != state.last_live_signature:
                        state.last_live_signature = signature
                    else:
                        continue
                    due.append((node, progress, now - state.started_at, persistent))
            for node, progress, elapsed, persistent in due:
                self._emit(node, progress, elapsed, persistent=persistent)

    @staticmethod
    def _snapshot(state: _NodeProgressState) -> ProgressSnapshot:
        if state.reader is None:
            return ProgressSnapshot(completed=state.completed, unit="out")
        return state.reader(state.completed)

    @staticmethod
    def _signature(progress: ProgressSnapshot) -> tuple[object, ...]:
        return (
            progress.completed,
            progress.total,
            progress.unit,
            progress.phase,
            progress.detail,
            progress.resource,
        )

    def _emit(
        self,
        node: _NodeProgressContext,
        progress: ProgressSnapshot,
        elapsed: float,
        persistent: bool,
    ) -> None:
        self._observer.on_node_progress(
            NodeProgressEvent(
                pipeline_name=node.pipeline_name,
                node_name=node.node_name,
                node_index=node.node_index,
                progress=progress,
                elapsed_seconds=elapsed,
                persistent=persistent,
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


def run_pipeline(
    context: PipelineContext,
    pipeline: Pipeline,
    *,
    seed: Iterable[Any] | None = None,
    observer: PipelineObserver | None = None,
) -> Generator[Any, None, None]:
    """Run a source followed by ordered stream stages."""

    active_observer = _resolve_observer(context, observer)
    if type(active_observer) is NoopPipelineObserver:
        yield from _run_unobserved(pipeline, seed)
        return

    yield from _run_observed(context, pipeline, seed, active_observer)


def _resolve_observer(
    context: PipelineContext,
    observer: PipelineObserver | None,
) -> PipelineObserver:
    if observer is not None:
        return observer
    if context.pipeline_observer is not None:
        return context.pipeline_observer
    return _NOOP_OBSERVER


def _run_unobserved(
    pipeline: Pipeline,
    seed: Iterable[Any] | None,
) -> Iterator[Any]:
    stream = _build_stream(pipeline, seed, observer=None, progress=None)
    try:
        yield from stream
    finally:
        _close_iterator(stream)


def _run_observed(
    context: PipelineContext,
    pipeline: Pipeline,
    seed: Iterable[Any] | None,
    observer: PipelineObserver,
) -> Iterator[Any]:
    start_time = time.perf_counter()
    output_items = 0
    status: RunStatus = "success"
    error_type: str | None = None
    error_message: str | None = None
    stream: Iterable[Any] = ()
    iterator: Iterator[Any] = iter(())
    started = False
    progress = _RunProgress(
        observer,
        resolve_heartbeat_interval_seconds(context.heartbeat_interval_seconds),
    )

    try:
        observer.on_pipeline_start(
            pipeline.name,
            pipeline.node_count,
            pipeline.summary,
        )
        started = True
        progress.start()
        stream = _build_stream(pipeline, seed, observer=observer, progress=progress)
        iterator = iter(stream)
        while True:
            try:
                item = next(iterator)
            except StopIteration:
                break
            output_items += 1
            yield item
    except KeyboardInterrupt as exc:
        status = "error"
        error_type = type(exc).__name__
        error_message = _error_message(exc)
        raise
    except Exception as exc:
        status = "error"
        error_type = type(exc).__name__
        error_message = _error_message(exc)
        raise
    finally:
        pipeline_failed = status == "error"
        close_error: BaseException | None = None
        try:
            _close_iterator(iterator)
            if iterator is not stream:
                _close_iterator(stream)
        except BaseException as exc:
            close_error = exc
            if not pipeline_failed:
                status = "error"
                error_type = type(exc).__name__
                error_message = _error_message(exc)

        progress_error: RuntimeError | None = None
        try:
            progress.stop()
        except RuntimeError as exc:
            progress_error = exc
            if not pipeline_failed and close_error is None:
                status = "error"
                error_type = type(exc).__name__
                error_message = _error_message(exc)

        if started:
            observer.on_pipeline_end(
                PipelineRunEvent(
                    pipeline_name=pipeline.name,
                    node_count=pipeline.node_count,
                    output_items=output_items,
                    elapsed_seconds=time.perf_counter() - start_time,
                    status=status,
                    error_type=error_type,
                    error_message=error_message,
                )
            )
        if not pipeline_failed and close_error is not None:
            raise close_error
        if not pipeline_failed and progress_error is not None:
            raise progress_error


def _build_stream(
    pipeline: Pipeline,
    seed: Iterable[Any] | None,
    observer: PipelineObserver | None,
    progress: _RunProgress | None,
) -> Iterable[Any]:
    nodes = pipeline.nodes
    stream: Iterable[Any]
    if nodes and isinstance(nodes[0], SourceNode):
        if seed is not None:
            raise ValueError(
                f"Pipeline '{pipeline.name}' cannot use both a source and a seed."
            )
        stream = _wrap_node(
            pipeline.name,
            nodes[0],
            0,
            upstream=None,
            observer=observer,
            progress=progress,
        )
        remaining = nodes[1:]
    else:
        if seed is None:
            if nodes:
                raise ValueError(
                    f"Pipeline '{pipeline.name}' requires a source or a seed."
                )
            stream = ()
        else:
            stream = seed
        remaining = nodes

    offset = len(nodes) - len(remaining)
    for index, node in enumerate(remaining, start=offset):
        if isinstance(node, SourceNode):
            raise ValueError(
                f"Pipeline '{pipeline.name}' source node '{node.name}' must be first."
            )
        stream = _wrap_node(
            pipeline.name,
            node,
            index,
            upstream=stream,
            observer=observer,
            progress=progress,
        )
    return stream


def _wrap_node(
    pipeline_name: str,
    node: Node,
    node_index: int,
    upstream: Iterable[Any] | None,
    observer: PipelineObserver | None,
    progress: _RunProgress | None,
) -> Iterator[Any]:
    if observer is None or progress is None:
        return _unobserved_node(node, upstream)
    return _observed_node(
        pipeline_name,
        node,
        node_index,
        upstream,
        observer,
        progress,
    )


def _open_node(node: Node, upstream: Iterable[Any] | None) -> Iterable[Any]:
    if isinstance(node, SourceNode):
        return node.open()
    if upstream is None:
        raise RuntimeError(f"Pipeline node '{node.name}' has no input stream.")
    return node.apply(iter(upstream))


def _unobserved_node(
    node: Node,
    upstream: Iterable[Any] | None,
) -> Iterator[Any]:
    produced: Iterable[Any] = ()
    iterator: Iterator[Any] = iter(())
    try:
        produced = _open_node(node, upstream)
        if produced is None:
            raise TypeError(
                f"Pipeline node '{node.name}' returned None; return an iterable."
            )
        iterator = iter(produced)
        yield from iterator
    finally:
        _close_iterator(iterator)
        if iterator is not produced:
            _close_iterator(produced)
        if upstream is not None and produced is not upstream:
            _close_iterator(upstream)


def _observed_node(
    pipeline_name: str,
    node: Node,
    node_index: int,
    upstream: Iterable[Any] | None,
    observer: PipelineObserver,
    progress: _RunProgress,
) -> Iterator[Any]:
    context = _NodeProgressContext(pipeline_name, node.name, node_index)
    start_time = time.perf_counter()
    output_items = 0
    status: RunStatus = "success"
    error_type: str | None = None
    error_message: str | None = None
    produced: Iterable[Any] = ()
    iterator: Iterator[Any] = iter(())
    started = False

    try:
        progress_state = progress.start_node(context, node.progress)
        observer.on_node_start(pipeline_name, node.name, node_index)
        started = True

        previous = progress.active_node
        progress.active_node = context
        try:
            produced = _open_node(node, upstream)
        finally:
            progress.active_node = previous
        if produced is None:
            raise TypeError(
                f"Pipeline node '{node.name}' returned None; return an iterable."
            )
        iterator = iter(produced)

        while True:
            previous = progress.active_node
            progress.active_node = context
            try:
                item = next(iterator)
            except StopIteration:
                break
            finally:
                progress.active_node = previous
            output_items += 1
            progress_state.completed = output_items
            yield item
    except KeyboardInterrupt as exc:
        status = "error"
        error_type = type(exc).__name__
        error_message = _error_message(exc)
        raise
    except Exception as exc:
        status = "error"
        error_type = type(exc).__name__
        error_message = _error_message(exc)
        raise
    finally:
        try:
            _close_iterator(iterator)
            if iterator is not produced:
                _close_iterator(produced)
            if upstream is not None and produced is not upstream:
                _close_iterator(upstream)
        finally:
            progress.clear(context)
        if started:
            observer.on_node_end(
                NodeExecutionEvent(
                    pipeline_name=pipeline_name,
                    node_name=node.name,
                    node_index=node_index,
                    output_items=output_items,
                    elapsed_seconds=time.perf_counter() - start_time,
                    status=status,
                    error_type=error_type,
                    error_message=error_message,
                )
            )
