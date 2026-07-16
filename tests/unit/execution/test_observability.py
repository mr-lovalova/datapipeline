from pathlib import Path

import pytest

import datapipeline.execution.observability as observability
from datapipeline.execution.observability import (
    FileResult,
    OperationFinished,
    OperationProgress,
    OperationProgressTracker,
    OperationStarted,
    current_operation_observer,
    emit_file_result,
    emit_operation_progress,
    operation_observer,
    operation_scope,
)


class _CaptureObserver:
    def __init__(self) -> None:
        self.started: list[OperationStarted] = []
        self.finished: list[OperationFinished] = []
        self.file_results: list[FileResult] = []
        self.progress: list[OperationProgress] = []

    def emit_started(self, event: OperationStarted) -> None:
        self.started.append(event)

    def emit_finished(self, event: OperationFinished) -> None:
        self.finished.append(event)

    def emit_file_result(self, result: FileResult) -> None:
        self.file_results.append(result)

    def emit_progress(self, event: OperationProgress) -> None:
        self.progress.append(event)


def test_observer_routes_operation_lifecycle_results_and_progress(monkeypatch) -> None:
    times = iter((3.0, 4.25))
    monkeypatch.setattr(observability.time, "perf_counter", lambda: next(times))
    observer = _CaptureObserver()

    assert current_operation_observer() is None
    assert emit_file_result("Output", Path("/tmp/out.jsonl")) is False
    assert emit_operation_progress("outside", 1, 1, "rows") is False

    with operation_observer(observer):
        assert current_operation_observer() is observer
        assert emit_file_result(
            "Model grid",
            Path("/tmp/model_grid.jsonl"),
        )
        with operation_scope("build:model_grid"):
            assert emit_operation_progress("write", 1, 3, "rows")

    assert current_operation_observer() is None
    assert observer.started == [OperationStarted("build:model_grid")]
    assert observer.finished == [
        OperationFinished(
            "build:model_grid",
            "success",
            elapsed_seconds=1.25,
        )
    ]
    assert observer.file_results == [
        FileResult("Model grid", Path("/tmp/model_grid.jsonl"))
    ]
    assert observer.progress == [
        OperationProgress(
            name="build:model_grid",
            step="write",
            step_elapsed_seconds=1,
            completed=3,
            unit="rows",
        ),
    ]


def test_operation_scope_emits_failure_and_restores_progress_context(
    monkeypatch,
) -> None:
    times = iter((5.0, 5.5))
    monkeypatch.setattr(observability.time, "perf_counter", lambda: next(times))
    observer = _CaptureObserver()

    with operation_observer(observer):
        with pytest.raises(ValueError, match="bad input"):
            with operation_scope("serve:test"):
                raise ValueError("  bad input  ")

        assert emit_operation_progress("after", 1, 1, "rows") is False

    assert observer.started == [OperationStarted("serve:test")]
    assert observer.finished == [
        OperationFinished(
            "serve:test",
            "error",
            elapsed_seconds=0.5,
            error_type="ValueError",
            error_message="  bad input  ",
        )
    ]
    assert observer.file_results == []
    assert observer.progress == []


def test_operation_progress_tracker_preserves_interval_counts_and_unit(
    monkeypatch,
) -> None:
    times = iter((0.0, 0.5, 1.0, 1.4, 2.1))
    monkeypatch.setattr(observability.time, "perf_counter", lambda: next(times))
    emitted: list[tuple[str, float, int, str]] = []

    def capture_progress(
        step: str,
        elapsed_seconds: float,
        completed: int,
        unit: str,
    ) -> bool:
        emitted.append((step, elapsed_seconds, completed, unit))
        return True

    monkeypatch.setattr(observability, "emit_operation_progress", capture_progress)
    progress = OperationProgressTracker("write", "rows", interval_seconds=1)

    progress.advance(2)
    progress.advance()
    progress.advance(4)
    progress.advance()

    assert emitted == [
        ("write", 1.0, 3, "rows"),
        ("write", 2.1, 8, "rows"),
    ]


def test_operation_progress_tracker_rejects_negative_interval() -> None:
    with pytest.raises(ValueError, match="interval_seconds must be non-negative"):
        OperationProgressTracker("write", "rows", interval_seconds=-0.1)
