import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, kw_only=True)
class _OperationEvent:
    name: str
    depth: int = 0


@dataclass(frozen=True, kw_only=True)
class OperationInfo(_OperationEvent):
    info_line: str


@dataclass(frozen=True, kw_only=True)
class OperationProgressEvent(_OperationEvent):
    step: str
    message: str


OperationEvent = OperationInfo | OperationProgressEvent


class OperationObserver(Protocol):
    def emit_operation_event(self, event: OperationEvent) -> None: ...


@dataclass(frozen=True)
class _OperationContext:
    name: str
    depth: int
    observer: OperationObserver | None


_CURRENT_OPERATION_OBSERVER: ContextVar[OperationObserver | None] = ContextVar(
    "datapipeline_current_operation_observer",
    default=None,
)
_CURRENT_OPERATION_CONTEXT: ContextVar[_OperationContext | None] = ContextVar(
    "datapipeline_current_operation_context",
    default=None,
)


def current_operation_observer() -> OperationObserver | None:
    return _CURRENT_OPERATION_OBSERVER.get()


@contextmanager
def operation_observer(observer: OperationObserver):
    token = _CURRENT_OPERATION_OBSERVER.set(observer)
    try:
        yield
    finally:
        _CURRENT_OPERATION_OBSERVER.reset(token)


@contextmanager
def operation_scope(name: str, depth: int = 0):
    operation_depth = max(0, int(depth))
    observer = current_operation_observer()
    token = _CURRENT_OPERATION_CONTEXT.set(
        _OperationContext(name=name, depth=operation_depth, observer=observer)
    )
    try:
        yield
    finally:
        _CURRENT_OPERATION_CONTEXT.reset(token)


def emit_operation_info(line: str) -> bool:
    context = _CURRENT_OPERATION_CONTEXT.get()
    if context is None or context.observer is None:
        return False
    context.observer.emit_operation_event(
        OperationInfo(
            name=context.name,
            depth=context.depth,
            info_line=line,
        )
    )
    return True


def emit_operation_progress(step: str, message: str) -> bool:
    context = _CURRENT_OPERATION_CONTEXT.get()
    if context is None or context.observer is None:
        return False
    context.observer.emit_operation_event(
        OperationProgressEvent(
            name=context.name,
            depth=context.depth,
            step=step,
            message=message,
        )
    )
    return True


class OperationProgress:
    def __init__(self, step: str, interval_seconds: float) -> None:
        interval = float(interval_seconds)
        if interval < 0:
            raise ValueError("interval_seconds must be non-negative")
        self._step = step
        self._interval_seconds = interval
        self._started_at = time.perf_counter()
        self._last_emit_at = self._started_at
        self._items = 0

    def advance(self, items: int = 1) -> None:
        self._items += int(items)
        if self._interval_seconds <= 0:
            return
        now = time.perf_counter()
        if now - self._last_emit_at < self._interval_seconds:
            return
        self._last_emit_at = now
        elapsed = now - self._started_at
        emit_operation_progress(
            self._step,
            f"running elapsed={elapsed:.0f}s items={self._items}",
        )


OperationProgressTracker = OperationProgress


__all__ = [
    "OperationEvent",
    "OperationInfo",
    "OperationObserver",
    "OperationProgress",
    "OperationProgressEvent",
    "OperationProgressTracker",
    "emit_operation_info",
    "emit_operation_progress",
    "operation_observer",
    "operation_scope",
]
