import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol


OperationStatus = Literal["success", "error"]


@dataclass(frozen=True)
class OperationStarted:
    name: str


@dataclass(frozen=True)
class OperationProgress:
    name: str
    step: str
    step_elapsed_seconds: float
    completed: int
    unit: str


@dataclass(frozen=True)
class OperationFinished:
    name: str
    status: OperationStatus
    elapsed_seconds: float
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class FileResult:
    label: str
    path: Path


class OperationObserver(Protocol):
    def emit_started(self, event: OperationStarted) -> None: ...

    def emit_finished(self, event: OperationFinished) -> None: ...

    def emit_file_result(self, result: FileResult) -> None: ...

    def emit_progress(self, event: OperationProgress) -> None: ...


_CURRENT_OPERATION_OBSERVER: ContextVar[OperationObserver | None] = ContextVar(
    "datapipeline_current_operation_observer",
    default=None,
)
_CURRENT_OPERATION_NAME: ContextVar[str | None] = ContextVar(
    "datapipeline_current_operation_name",
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
def operation_scope(name: str):
    observer = current_operation_observer()
    started_at = time.perf_counter()
    if observer is not None:
        observer.emit_started(OperationStarted(name))
    token = _CURRENT_OPERATION_NAME.set(name)
    try:
        yield
    except BaseException as exc:
        if observer is not None:
            observer.emit_finished(
                OperationFinished(
                    name=name,
                    status="error",
                    elapsed_seconds=time.perf_counter() - started_at,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            )
        raise
    else:
        if observer is not None:
            observer.emit_finished(
                OperationFinished(
                    name=name,
                    status="success",
                    elapsed_seconds=time.perf_counter() - started_at,
                )
            )
    finally:
        _CURRENT_OPERATION_NAME.reset(token)


def emit_file_result(
    label: str,
    path: Path,
) -> bool:
    observer = current_operation_observer()
    if observer is None:
        return False
    observer.emit_file_result(FileResult(label, path))
    return True


def emit_operation_progress(
    step: str,
    step_elapsed_seconds: float,
    completed: int,
    unit: str,
) -> bool:
    name = _CURRENT_OPERATION_NAME.get()
    observer = current_operation_observer()
    if name is None or observer is None:
        return False
    observer.emit_progress(
        OperationProgress(
            name=name,
            step=step,
            step_elapsed_seconds=step_elapsed_seconds,
            completed=completed,
            unit=unit,
        )
    )
    return True


class OperationProgressTracker:
    def __init__(self, step: str, unit: str, interval_seconds: float) -> None:
        interval = float(interval_seconds)
        if interval < 0:
            raise ValueError("interval_seconds must be non-negative")
        self._step = step
        self._unit = unit
        self._interval_seconds = interval
        self._started_at = time.perf_counter()
        self._last_emit_at = self._started_at
        self._completed = 0

    def advance(self, count: int = 1) -> None:
        self._completed += int(count)
        if self._interval_seconds <= 0:
            return
        now = time.perf_counter()
        if now - self._last_emit_at < self._interval_seconds:
            return
        self._last_emit_at = now
        elapsed = now - self._started_at
        emit_operation_progress(self._step, elapsed, self._completed, self._unit)
