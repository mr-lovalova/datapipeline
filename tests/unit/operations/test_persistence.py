import gzip
import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

import datapipeline.operations.persistence as persistence
from datapipeline.artifacts.registry import ArtifactRegistry
from datapipeline.cli.visuals.execution import make_operation_observer
from datapipeline.cli.visuals.execution_context import (
    reset_current_execution_event_handler,
    set_current_execution_event_handler,
)
from datapipeline.execution.observability import (
    FileResult,
    operation_observer,
    operation_scope,
)
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import (
    ArtifactOutput,
    RoutedRuntimeOutput,
    RuntimeOutput,
    RuntimeOutputBatch,
    persist_artifact_output,
    persist_runtime_result,
)


class _CaptureHandler:
    def __init__(self) -> None:
        self.events = []

    def __call__(self, event) -> None:
        self.events.append(event)


class _ClosableRows:
    def __init__(
        self,
        rows=(),
        *,
        iteration_error: Exception | None = None,
        close_error: Exception | None = None,
    ) -> None:
        self._rows = iter(rows)
        self._iteration_error = iteration_error
        self._close_error = close_error
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._iteration_error is not None:
            raise self._iteration_error
        return next(self._rows)

    def close(self) -> None:
        self.closed = True
        if self._close_error is not None:
            raise self._close_error


class _IterFailureRows:
    def __init__(self) -> None:
        self.closed = False

    def __iter__(self):
        raise RuntimeError("iterator failed")

    def close(self) -> None:
        self.closed = True


def test_persist_artifact_output_requires_declared_existing_file(tmp_path) -> None:
    runtime = SimpleNamespace(
        artifacts_root=tmp_path,
        artifacts=ArtifactRegistry(tmp_path),
    )

    with pytest.raises(ValueError, match="but its operation declares"):
        persist_artifact_output(
            ArtifactOutput(relative_path="other.json"),
            artifact_key="snapshot",
            expected_relative_path="snapshot.json",
            runtime=runtime,
        )

    with pytest.raises(RuntimeError, match="did not create its declared output"):
        persist_artifact_output(
            ArtifactOutput(relative_path="snapshot.json"),
            artifact_key="snapshot",
            expected_relative_path="snapshot.json",
            runtime=runtime,
        )


def test_persist_artifact_output_snapshots_declared_file(tmp_path) -> None:
    output = tmp_path / "snapshot.json"
    output.write_text("{}", encoding="utf-8")
    runtime = SimpleNamespace(
        artifacts_root=tmp_path,
        artifacts=ArtifactRegistry(tmp_path),
    )

    info = persist_artifact_output(
        ArtifactOutput(relative_path="snapshot.json", meta={"rows": 1}),
        artifact_key="snapshot",
        expected_relative_path="snapshot.json",
        runtime=runtime,
    )

    assert info is not None
    assert info.relative_path == "snapshot.json"
    assert tuple(file.relative_path for file in info.files) == ("snapshot.json",)
    assert not runtime.artifacts.has("snapshot")


def test_persist_artifact_output_validates_companion_files(tmp_path) -> None:
    output = tmp_path / "manifest.json"
    companion = tmp_path / "manifest.shards/000000.jsonl.gz"
    output.write_text("{}", encoding="utf-8")
    companion.parent.mkdir()
    companion.write_bytes(b"shard")
    runtime = SimpleNamespace(
        artifacts_root=tmp_path,
        artifacts=ArtifactRegistry(tmp_path),
    )

    info = persist_artifact_output(
        ArtifactOutput(
            relative_path="manifest.json",
            companion_paths=("manifest.shards/000000.jsonl.gz",),
        ),
        artifact_key="variable_records",
        expected_relative_path="manifest.json",
        runtime=runtime,
    )

    assert info is not None
    assert tuple(file.relative_path for file in info.files) == (
        "manifest.json",
        "manifest.shards/000000.jsonl.gz",
    )


def test_persist_artifact_output_rejects_escaping_companion(tmp_path) -> None:
    (tmp_path / "manifest.json").write_text("{}", encoding="utf-8")
    runtime = SimpleNamespace(
        artifacts_root=tmp_path,
        artifacts=ArtifactRegistry(tmp_path),
    )

    with pytest.raises(ValueError, match="must be relative"):
        persist_artifact_output(
            ArtifactOutput(
                relative_path="manifest.json",
                companion_paths=("../outside.jsonl.gz",),
            ),
            artifact_key="variable_records",
            expected_relative_path="manifest.json",
            runtime=runtime,
        )


def test_persist_artifact_output_rejects_case_colliding_companion(tmp_path) -> None:
    (tmp_path / "manifest.json").write_text("{}", encoding="utf-8")
    runtime = SimpleNamespace(artifacts_root=tmp_path)

    with pytest.raises(ValueError, match="output paths must be unique"):
        persist_artifact_output(
            ArtifactOutput(
                relative_path="manifest.json",
                companion_paths=("MANIFEST.json",),
            ),
            artifact_key="variable_records",
            expected_relative_path="manifest.json",
            runtime=runtime,
        )


def test_failed_runtime_write_preserves_existing_file(tmp_path) -> None:
    destination = tmp_path / "out.jsonl"
    destination.write_text("previous\n", encoding="utf-8")

    def rows():
        yield {"value": 1}
        raise RuntimeError("boom")

    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=destination,
    )

    with pytest.raises(RuntimeError, match="boom"):
        persist_runtime_result(
            RuntimeOutput(rows=rows()),
            target=target,
            logger=logging.getLogger(__name__),
        )

    assert destination.read_text(encoding="utf-8") == "previous\n"


def test_failed_runtime_write_closes_rows(tmp_path) -> None:
    closed = False

    def rows():
        nonlocal closed
        try:
            yield {"value": object()}
        finally:
            closed = True

    stream = rows()
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=tmp_path / "out.jsonl",
    )

    with pytest.raises(TypeError, match="Unsupported output value type"):
        persist_runtime_result(
            RuntimeOutput(rows=stream),
            target=target,
            logger=logging.getLogger(__name__),
        )

    assert closed


def test_runtime_writer_open_failure_closes_rows(monkeypatch, tmp_path) -> None:
    rows = _ClosableRows()
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=tmp_path / "out.jsonl",
    )

    def fail_writer_open(_target):
        raise OSError("open failed")

    monkeypatch.setattr(persistence, "writer_factory", fail_writer_open)

    with pytest.raises(OSError, match="open failed"):
        persist_runtime_result(
            RuntimeOutput(rows=rows),
            target=target,
            logger=logging.getLogger(__name__),
        )

    assert rows.closed


def test_runtime_row_cleanup_failure_aborts_output(tmp_path) -> None:
    destination = tmp_path / "out.jsonl"
    destination.write_text("previous\n", encoding="utf-8")
    rows = _ClosableRows(
        ({"value": 1},),
        close_error=OSError("cleanup failed"),
    )
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=destination,
    )

    with pytest.raises(OSError, match="cleanup failed"):
        persist_runtime_result(
            RuntimeOutput(rows=rows),
            target=target,
            logger=logging.getLogger(__name__),
        )

    assert rows.closed
    assert destination.read_text(encoding="utf-8") == "previous\n"


def test_runtime_row_cleanup_does_not_replace_processing_failure(tmp_path) -> None:
    rows = _ClosableRows(
        iteration_error=RuntimeError("processing failed"),
        close_error=OSError("cleanup failed"),
    )
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=tmp_path / "out.jsonl",
    )

    with pytest.raises(RuntimeError, match="processing failed"):
        persist_runtime_result(
            RuntimeOutput(rows=rows),
            target=target,
            logger=logging.getLogger(__name__),
        )

    assert rows.closed


def test_runtime_iterator_failure_closes_rows(tmp_path) -> None:
    rows = _IterFailureRows()
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=tmp_path / "out.jsonl",
    )

    with pytest.raises(RuntimeError, match="iterator failed"):
        persist_runtime_result(
            RuntimeOutput(rows=rows),
            target=target,
            logger=logging.getLogger(__name__),
        )

    assert rows.closed


def test_runtime_batch_failure_closes_pending_rows(tmp_path) -> None:
    pending_rows = _ClosableRows()
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=tmp_path / "out.jsonl",
    )
    result = RuntimeOutputBatch(
        outputs=(
            RuntimeOutput(
                rows=_ClosableRows(iteration_error=RuntimeError("processing failed")),
                target=target,
            ),
            RuntimeOutput(rows=pending_rows, target=target),
        )
    )

    with pytest.raises(RuntimeError, match="processing failed"):
        persist_runtime_result(
            result,
            target=None,
            logger=logging.getLogger(__name__),
        )

    assert pending_rows.closed


def test_runtime_batch_callback_does_not_replace_persistence_failure(
    tmp_path,
) -> None:
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=tmp_path / "out.jsonl",
    )

    def fail_completion(_success: bool) -> None:
        raise OSError("completion failed")

    result = RuntimeOutputBatch(
        outputs=(
            RuntimeOutput(
                rows=_ClosableRows(iteration_error=RuntimeError("processing failed")),
                target=target,
            ),
        ),
        on_complete=fail_completion,
    )

    with pytest.raises(RuntimeError, match="processing failed"):
        persist_runtime_result(
            result,
            target=None,
            logger=logging.getLogger(__name__),
        )


def test_routed_runtime_output_rejects_colliding_destinations(tmp_path) -> None:
    targets = {
        output_id: OutputTarget(
            transport="fs",
            format="jsonl",
            view="raw",
            encoding="utf-8",
            destination=tmp_path / filename,
        )
        for output_id, filename in (
            ("train", "SPLIT.jsonl"),
            ("test", "split.jsonl"),
        )
    }

    with pytest.raises(ValueError, match="resolve to the same destination"):
        persist_runtime_result(
            RoutedRuntimeOutput(
                rows=iter(()),
                targets=targets,
                output_for_row=lambda row: row["output"],
            ),
            target=None,
            logger=logging.getLogger(__name__),
        )

    assert list(tmp_path.iterdir()) == []


def test_invalid_routed_runtime_output_closes_rows() -> None:
    rows = _ClosableRows()

    with pytest.raises(ValueError, match="at least one target"):
        persist_runtime_result(
            RoutedRuntimeOutput(
                rows=rows,
                targets={},
                output_for_row=lambda _row: None,
            ),
            target=None,
            logger=logging.getLogger(__name__),
        )

    assert rows.closed


def test_runtime_persistence_emits_flat_output(tmp_path) -> None:
    destination = tmp_path / "out.jsonl"
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=destination,
    )
    capture = _CaptureHandler()
    token = set_current_execution_event_handler(capture)
    try:
        logger = logging.getLogger(__name__)
        observer = make_operation_observer(logger)
        with operation_observer(observer):
            with operation_scope("serve:train"):
                persist_runtime_result(
                    RuntimeOutput(rows=iter([{"value": 1}])),
                    target=target,
                    heartbeat_interval_seconds=0,
                    logger=logger,
                )
    finally:
        reset_current_execution_event_handler(token)

    outputs = [event for event in capture.events if isinstance(event, FileResult)]
    assert len(outputs) == 1
    assert outputs[0].label == "Output"
    assert outputs[0].path == destination


def test_runtime_payload_emits_output_path(monkeypatch, tmp_path) -> None:
    destination = tmp_path / "coverage.json"
    results: list[tuple[str, Path]] = []
    monkeypatch.setattr(
        persistence,
        "emit_file_result",
        lambda label, path: results.append((label, path)),
    )

    persist_runtime_result(
        RuntimeOutput(payload={"covered": True}),
        target=OutputTarget(
            transport="fs",
            format="jsonl",
            view="raw",
            encoding="utf-8",
            destination=destination,
        ),
        logger=logging.getLogger(__name__),
    )

    assert results == [("Output", destination)]


def test_runtime_persistence_reports_stdout(capsys, caplog) -> None:
    logger = logging.getLogger("datapipeline.tests.persistence.stdout")
    with caplog.at_level(logging.INFO, logger=logger.name):
        persist_runtime_result(
            RuntimeOutput(rows=({"value": 1},)),
            target=OutputTarget(
                transport="stdout",
                format="jsonl",
                view="raw",
                encoding=None,
                destination=None,
            ),
            logger=logger,
        )

    assert capsys.readouterr().out == '{"value": 1}\n'
    assert [record.getMessage() for record in caplog.records] == ["Output: stdout"]


def test_runtime_persistence_reports_html_path(monkeypatch, tmp_path) -> None:
    destination = tmp_path / "matrix.html"
    results: list[tuple[str, Path]] = []
    monkeypatch.setattr(
        persistence,
        "emit_file_result",
        lambda label, path: results.append((label, path)),
    )

    persist_runtime_result(
        RuntimeOutput(render_html=lambda: "<html></html>"),
        target=OutputTarget(
            transport="fs",
            format="html",
            view="flat",
            encoding="utf-8",
            destination=destination,
        ),
        logger=logging.getLogger(__name__),
    )

    assert destination.read_text(encoding="utf-8") == "<html></html>"
    assert results == [("Output", destination)]


def test_html_output_cleanup_failure_prevents_commit(tmp_path) -> None:
    destination = tmp_path / "matrix.html"
    destination.write_text("previous", encoding="utf-8")
    rows = _ClosableRows(close_error=OSError("cleanup failed"))

    with pytest.raises(OSError, match="cleanup failed"):
        persist_runtime_result(
            RuntimeOutput(
                rows=rows,
                render_html=lambda: "replacement",
            ),
            target=OutputTarget(
                transport="fs",
                format="html",
                view="flat",
                encoding="utf-8",
                destination=destination,
            ),
            logger=logging.getLogger(__name__),
        )

    assert rows.closed
    assert destination.read_text(encoding="utf-8") == "previous"


def test_failed_html_commit_preserves_existing_file(monkeypatch, tmp_path) -> None:
    destination = tmp_path / "matrix.html"
    destination.write_text("previous", encoding="utf-8")

    def fail_commit(*_args):
        raise OSError("commit failed")

    monkeypatch.setattr(
        "datapipeline.io.sinks.files._commit_temp_file",
        fail_commit,
    )

    with pytest.raises(OSError, match="commit failed"):
        persist_runtime_result(
            RuntimeOutput(render_html=lambda: "replacement"),
            target=OutputTarget(
                transport="fs",
                format="html",
                view="flat",
                encoding="utf-8",
                destination=destination,
            ),
            logger=logging.getLogger(__name__),
        )

    assert destination.read_text(encoding="utf-8") == "previous"
    assert list(tmp_path.iterdir()) == [destination]


def test_routed_runtime_output_routes_rows_to_output_targets(
    monkeypatch,
    tmp_path,
) -> None:
    train_path = tmp_path / "train.jsonl"
    val_path = tmp_path / "val.jsonl"
    train_target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=train_path,
    )
    val_target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=val_path,
    )
    rows = [
        {"output": "train", "value": 1},
        {"output": "val", "value": 2},
        {"output": "test", "value": 3},
        {"output": None, "value": 4},
    ]
    results: list[tuple[str, Path]] = []
    monkeypatch.setattr(
        persistence,
        "emit_file_result",
        lambda label, path: results.append((label, path)),
    )

    persist_runtime_result(
        RoutedRuntimeOutput(
            rows=iter(rows),
            targets={"train": train_target, "val": val_target},
            output_for_row=lambda row: row["output"],
        ),
        target=None,
        logger=logging.getLogger(__name__),
    )

    train_rows = [
        json.loads(line) for line in train_path.read_text(encoding="utf-8").splitlines()
    ]
    val_rows = [
        json.loads(line) for line in val_path.read_text(encoding="utf-8").splitlines()
    ]
    assert train_rows == [{"output": "train", "value": 1}]
    assert val_rows == [{"output": "val", "value": 2}]
    assert results == [
        ("train", train_path),
        ("val", val_path),
    ]


def test_routed_runtime_output_writes_gzip_targets(tmp_path) -> None:
    targets = {
        output_id: OutputTarget(
            transport="fs",
            format="jsonl",
            view="raw",
            encoding="utf-8",
            destination=tmp_path / f"{output_id}.jsonl.gz",
            compression="gzip",
        )
        for output_id in ("train", "validation")
    }
    rows = [
        {"output": "train", "value": 1},
        {"output": "validation", "value": 2},
    ]

    persist_runtime_result(
        RoutedRuntimeOutput(
            rows=iter(rows),
            targets=targets,
            output_for_row=lambda row: row["output"],
        ),
        target=None,
        logger=logging.getLogger(__name__),
    )

    for output_id, expected in (("train", rows[:1]), ("validation", rows[1:])):
        destination = targets[output_id].destination
        assert destination is not None
        with gzip.open(destination, "rt", encoding="utf-8") as stream:
            assert [json.loads(line) for line in stream] == expected


def test_routed_runtime_output_limit_applies_per_output(tmp_path) -> None:
    train_path = tmp_path / "train.jsonl"
    val_path = tmp_path / "val.jsonl"
    train_target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=train_path,
    )
    val_target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=val_path,
    )
    rows = [
        {"split": "ignored", "value": 0},
        {"split": "train", "value": 1},
        {"split": "train", "value": 2},
        {"split": "val", "value": 3},
        {"split": "val", "value": 4},
    ]

    persist_runtime_result(
        RoutedRuntimeOutput(
            rows=iter(rows),
            targets={"train": train_target, "val": val_target},
            output_for_row=lambda row: row["split"],
            limit_per_output=1,
        ),
        target=None,
        logger=logging.getLogger(__name__),
    )

    train_rows = [
        json.loads(line) for line in train_path.read_text(encoding="utf-8").splitlines()
    ]
    val_rows = [
        json.loads(line) for line in val_path.read_text(encoding="utf-8").splitlines()
    ]
    assert train_rows == [{"split": "train", "value": 1}]
    assert val_rows == [{"split": "val", "value": 3}]
