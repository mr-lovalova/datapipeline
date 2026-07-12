import pytest

import datapipeline.execution.observability as observability
from datapipeline.execution.observability import (
    OperationProgress,
    OperationProgressTracker,
    current_operation_observer,
    emit_operation_progress,
    emit_operation_result,
    operation_observer,
    operation_scope,
)


class _CaptureObserver:
    def __init__(self) -> None:
        self.results: list[str] = []
        self.progress: list[tuple[str, str, str]] = []

    def emit_result(self, line: str) -> None:
        self.results.append(line)

    def emit_progress(self, name: str, step: str, message: str) -> None:
        self.progress.append((name, step, message))


def test_operation_scope_routes_result_and_progress() -> None:
    observer = _CaptureObserver()

    assert current_operation_observer() is None
    assert emit_operation_result("outside") is False
    assert emit_operation_progress("outside", "ignored") is False

    with operation_observer(observer):
        assert current_operation_observer() is observer
        with operation_scope("build:model_grid"):
            assert emit_operation_result("Model grid: /tmp/model_grid.jsonl")
            assert emit_operation_progress("write", "running items=3")

    assert current_operation_observer() is None
    assert observer.results == ["Model grid: /tmp/model_grid.jsonl"]
    assert observer.progress == [
        ("build:model_grid", "write", "running items=3"),
    ]


def test_operation_scope_restores_context_after_failure() -> None:
    observer = _CaptureObserver()

    with operation_observer(observer):
        with pytest.raises(ValueError, match="bad input"):
            with operation_scope("serve:test"):
                raise ValueError("  bad input  ")

        assert emit_operation_result("after failure") is False

    assert observer.results == []
    assert observer.progress == []


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
