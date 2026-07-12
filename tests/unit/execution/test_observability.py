import pytest

import datapipeline.execution.observability as observability
from datapipeline.execution.observability import (
    OperationEvent,
    OperationInfo,
    OperationProgress,
    OperationProgressEvent,
    OperationProgressTracker,
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


def test_operation_scope_emits_typed_detail_events() -> None:
    observer = _CaptureObserver()

    assert current_operation_observer() is None
    assert emit_operation_info("outside") is False
    assert emit_operation_progress("outside", "ignored") is False

    with operation_observer(observer):
        assert current_operation_observer() is observer
        with operation_scope("build:model_grid", depth=-1):
            assert emit_operation_info("materialized path=/tmp/model_grid.jsonl")
            assert emit_operation_progress("write", "running items=3")

    assert current_operation_observer() is None
    assert observer.events == [
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
    ]
    assert all(isinstance(event, OperationEvent) for event in observer.events)


def test_operation_scope_restores_detail_context_after_failure() -> None:
    observer = _CaptureObserver()

    with operation_observer(observer):
        with pytest.raises(ValueError, match="bad input"):
            with operation_scope("serve:test", depth=2):
                raise ValueError("  bad input  ")

        assert emit_operation_info("after failure") is False

    assert observer.events == []


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
