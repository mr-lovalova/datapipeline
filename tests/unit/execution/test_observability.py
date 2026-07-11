import pytest

import datapipeline.execution.observability as observability
from datapipeline.execution.observability import (
    OperationEvent,
    OperationFinished,
    OperationInfo,
    OperationProgress,
    OperationProgressEvent,
    OperationProgressTracker,
    OperationStarted,
    current_operation_observer,
    emit_operation_info,
    emit_operation_progress,
    operation_observer,
    operation_scope,
)


class _CaptureObserver:
    def __init__(self) -> None:
        self.events: list[OperationEvent] = []

    def emit_operation_event(self, event: OperationEvent) -> None:
        self.events.append(event)


def test_operation_scope_emits_typed_lifecycle_and_detail_events(monkeypatch) -> None:
    times = iter((10.0, 12.5))
    monkeypatch.setattr(observability.time, "perf_counter", lambda: next(times))
    observer = _CaptureObserver()

    assert current_operation_observer() is None
    assert emit_operation_info("outside") is False
    assert emit_operation_progress("outside", "ignored") is False

    with operation_observer(observer):
        assert current_operation_observer() is observer
        with operation_scope("build:model_grid", "core.artifact.ticks", depth=-1):
            assert emit_operation_info("materialized path=/tmp/model_grid.jsonl")
            assert emit_operation_progress("write", "running items=3")

    assert current_operation_observer() is None
    assert observer.events == [
        OperationStarted(
            name="build:model_grid",
            depth=0,
            entrypoint="core.artifact.ticks",
        ),
        OperationInfo(
            name="build:model_grid",
            depth=0,
            info_line="materialized path=/tmp/model_grid.jsonl",
        ),
        OperationProgressEvent(
            name="build:model_grid",
            depth=0,
            step="write",
            message="running items=3",
        ),
        OperationFinished(
            name="build:model_grid",
            depth=0,
            status="success",
            elapsed_seconds=2.5,
        ),
    ]
    assert all(isinstance(event, OperationEvent) for event in observer.events)


def test_operation_scope_reports_failure_and_restores_detail_context(
    monkeypatch,
) -> None:
    times = iter((20.0, 21.25))
    monkeypatch.setattr(observability.time, "perf_counter", lambda: next(times))
    observer = _CaptureObserver()

    with operation_observer(observer):
        with pytest.raises(ValueError, match="bad input"):
            with operation_scope("serve:test", "core.runtime.pipeline", depth=2):
                raise ValueError("  bad input  ")

        assert emit_operation_info("after failure") is False

    assert observer.events == [
        OperationStarted(
            name="serve:test",
            depth=2,
            entrypoint="core.runtime.pipeline",
        ),
        OperationFinished(
            name="serve:test",
            depth=2,
            status="error",
            error_type="ValueError",
            error_message="bad input",
            elapsed_seconds=1.25,
        ),
    ]


def test_operation_scope_restores_context_when_start_observer_fails() -> None:
    class FailingObserver:
        def emit_operation_event(self, event: OperationEvent) -> None:
            raise RuntimeError("observer failed")

    with operation_observer(FailingObserver()):
        with pytest.raises(RuntimeError, match="observer failed"):
            with operation_scope("build:schema", "core.artifact.schema"):
                pass

        assert emit_operation_info("after failure") is False


def test_operation_scope_does_not_install_context_when_clock_fails(monkeypatch) -> None:
    observer = _CaptureObserver()

    def fail_clock() -> float:
        raise RuntimeError("clock failed")

    monkeypatch.setattr(observability.time, "perf_counter", fail_clock)

    with operation_observer(observer):
        with pytest.raises(RuntimeError, match="clock failed"):
            with operation_scope("build:schema", "core.artifact.schema"):
                pass

        assert emit_operation_info("after failure") is False


def test_finish_observer_does_not_mask_operation_error() -> None:
    class FailingFinishObserver:
        def emit_operation_event(self, event: OperationEvent) -> None:
            if isinstance(event, OperationFinished):
                raise RuntimeError("finish observer failed")

    with operation_observer(FailingFinishObserver()):
        with pytest.raises(ValueError, match="body failed"):
            with operation_scope("build:schema", "core.artifact.schema"):
                raise ValueError("body failed")

        assert emit_operation_info("after failure") is False


def test_operation_progress_tracker_preserves_interval_and_item_counts(
    monkeypatch,
) -> None:
    times = iter((0.0, 0.5, 1.0, 1.4, 2.1))
    monkeypatch.setattr(observability.time, "perf_counter", lambda: next(times))
    emitted: list[tuple[str, str]] = []

    def capture_progress(step: str, message: str) -> bool:
        emitted.append((step, message))
        return True

    monkeypatch.setattr(observability, "emit_operation_progress", capture_progress)
    progress = OperationProgress("write", interval_seconds=1)

    progress.advance(2)
    progress.advance()
    progress.advance(4)
    progress.advance()

    assert emitted == [
        ("write", "running elapsed=1s items=3"),
        ("write", "running elapsed=2s items=8"),
    ]


def test_operation_progress_tracker_rejects_negative_interval() -> None:
    with pytest.raises(ValueError, match="interval_seconds must be non-negative"):
        OperationProgress("write", interval_seconds=-0.1)


def test_operation_progress_tracker_alias_preserves_new_name() -> None:
    assert OperationProgressTracker is OperationProgress
