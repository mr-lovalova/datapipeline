import logging
import json
from types import SimpleNamespace

import pytest

from datapipeline.cli.visuals.execution import (
    ExecutionEventSink,
    make_operation_observer,
)
from datapipeline.cli.visuals.execution_context import (
    reset_current_execution_event_sink,
    set_current_execution_event_sink,
)
from datapipeline.execution.observability import operation_observer, operation_scope
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import (
    ArtifactOutput,
    RuntimeOutput,
    SplitRuntimeOutput,
    persist_artifact_output,
    persist_runtime_result,
)
from datapipeline.services.artifacts import ArtifactManager


class _CaptureSink(ExecutionEventSink):
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)


def test_persist_artifact_output_requires_declared_existing_file(tmp_path) -> None:
    runtime = SimpleNamespace(
        artifacts_root=tmp_path,
        artifacts=ArtifactManager(tmp_path),
    )

    with pytest.raises(ValueError, match="but its task declares"):
        persist_artifact_output(
            ArtifactOutput(relative_path="other.json"),
            artifact_key="snapshot",
            expected_relative_path="snapshot.json",
            runtime=runtime,
            logger=logging.getLogger(__name__),
        )

    with pytest.raises(RuntimeError, match="did not create its declared output"):
        persist_artifact_output(
            ArtifactOutput(relative_path="snapshot.json"),
            artifact_key="snapshot",
            expected_relative_path="snapshot.json",
            runtime=runtime,
            logger=logging.getLogger(__name__),
        )


def test_persist_artifact_output_registers_declared_file(tmp_path) -> None:
    output = tmp_path / "snapshot.json"
    output.write_text("{}", encoding="utf-8")
    runtime = SimpleNamespace(
        artifacts_root=tmp_path,
        artifacts=ArtifactManager(tmp_path),
    )

    info = persist_artifact_output(
        ArtifactOutput(relative_path="snapshot.json", meta={"rows": 1}),
        artifact_key="snapshot",
        expected_relative_path="snapshot.json",
        runtime=runtime,
        logger=logging.getLogger(__name__),
    )

    assert info is not None
    assert info.relative_path == "snapshot.json"
    assert runtime.artifacts.has("snapshot")


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
            visuals="off",
            logger=logging.getLogger(__name__),
        )

    assert destination.read_text(encoding="utf-8") == "previous\n"


def test_runtime_persistence_emits_operation_saved_info(tmp_path) -> None:
    destination = tmp_path / "out.jsonl"
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=destination,
    )
    capture = _CaptureSink()
    token = set_current_execution_event_sink(capture)
    try:
        logger = logging.getLogger(__name__)
        observer = make_operation_observer(logger)
        with operation_observer(observer):
            with operation_scope("serve:train", "core.runtime.materialize"):
                persist_runtime_result(
                    RuntimeOutput(rows=iter([{"value": 1}])),
                    target=target,
                    visuals="off",
                    heartbeat_interval_seconds=0,
                    logger=logger,
                )
    finally:
        reset_current_execution_event_sink(token)

    infos = [event for event in capture.events if event.kind == "operation_info"]
    assert len(infos) == 1
    assert infos[0].dag_name == "serve:train"
    assert infos[0].info_line == f"saved path={destination} items=1"


def test_split_runtime_output_routes_rows_to_label_targets(tmp_path) -> None:
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
        {"split": "train", "value": 1},
        {"split": "val", "value": 2},
        {"split": "test", "value": 3},
    ]

    persist_runtime_result(
        SplitRuntimeOutput(
            rows=iter(rows),
            targets={"train": train_target, "val": val_target},
            label_for_row=lambda row: row["split"],
        ),
        target=None,
        visuals="off",
        logger=logging.getLogger(__name__),
    )

    train_rows = [
        json.loads(line) for line in train_path.read_text(encoding="utf-8").splitlines()
    ]
    val_rows = [
        json.loads(line) for line in val_path.read_text(encoding="utf-8").splitlines()
    ]
    assert train_rows == [{"split": "train", "value": 1}]
    assert val_rows == [{"split": "val", "value": 2}]


def test_split_runtime_output_limit_applies_per_target(tmp_path) -> None:
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
        SplitRuntimeOutput(
            rows=iter(rows),
            targets={"train": train_target, "val": val_target},
            label_for_row=lambda row: row["split"],
            limit_per_target=1,
        ),
        target=None,
        visuals="off",
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
