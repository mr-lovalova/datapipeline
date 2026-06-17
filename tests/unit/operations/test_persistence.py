import logging
import json

import pytest

from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import (
    RuntimeOutput,
    SplitRuntimeOutput,
    persist_runtime_result,
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
            visuals="off",
            logger=logging.getLogger(__name__),
            emit_message=lambda *args, **kwargs: None,
        )

    assert destination.read_text(encoding="utf-8") == "previous\n"


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
        emit_message=lambda *args, **kwargs: None,
    )

    train_rows = [
        json.loads(line)
        for line in train_path.read_text(encoding="utf-8").splitlines()
    ]
    val_rows = [
        json.loads(line)
        for line in val_path.read_text(encoding="utf-8").splitlines()
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
        emit_message=lambda *args, **kwargs: None,
    )

    train_rows = [
        json.loads(line)
        for line in train_path.read_text(encoding="utf-8").splitlines()
    ]
    val_rows = [
        json.loads(line)
        for line in val_path.read_text(encoding="utf-8").splitlines()
    ]
    assert train_rows == [{"split": "train", "value": 1}]
    assert val_rows == [{"split": "val", "value": 3}]
