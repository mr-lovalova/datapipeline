import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.execution import ExecutionConfig
from datapipeline.domain.record import TemporalRecord
from datapipeline.runtime import IngestRuntimeStream, Runtime
from datapipeline.services.materialize import materialize_stream_to_path


class _Source:
    def __init__(self, rows):
        self._rows = rows

    def stream(self):
        return iter(self._rows)


def _ts(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def _mapper(rows):
    for row in rows:
        rec = TemporalRecord(time=row["time"])
        rec.security_id = row["security_id"]
        rec.close = row["close"]
        yield rec


def _runtime(
    tmp_path: Path,
    rows: list[dict],
    partition_by: tuple[str, ...] = (),
) -> Runtime:
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
        execution=ExecutionConfig(),
    )
    runtime.streams["prices.raw"] = IngestRuntimeStream(
        source=_Source(rows),
        mapper=_mapper,
        transforms=(),
        partition_by=partition_by,
        presorted=False,
    )
    return runtime


@pytest.fixture
def one_row_runtime(tmp_path: Path) -> Runtime:
    return _runtime(
        tmp_path,
        [{"time": _ts(1), "security_id": "AAPL", "close": 10.0}],
    )


def test_materialize_stream_writes_jsonl_and_metadata(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(2), "security_id": "MSFT", "close": 20.0},
        {"time": _ts(1), "security_id": "AAPL", "close": 10.0},
    ]
    runtime = _runtime(tmp_path, rows)
    output = tmp_path / "interim" / "prices.materialized.jsonl"

    result = materialize_stream_to_path(
        runtime=runtime,
        stream_id="prices.raw",
        output=output,
    )

    assert result.count == 2
    payloads = [
        json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()
    ]
    assert [(row["security_id"], row["close"]) for row in payloads] == [
        ("AAPL", 10.0),
        ("MSFT", 20.0),
    ]
    assert (
        result.metadata
        == (tmp_path / "interim" / "prices.materialized.metadata.json").resolve()
    )
    assert json.loads(result.metadata.read_text(encoding="utf-8")) == {
        "rows": 2,
        "format": "jsonl",
        "encoding": "utf-8",
        "partition_by": [],
        "ordered_by": ["time"],
    }


def test_materialize_stream_preserves_partition_identity(
    tmp_path: Path,
) -> None:
    rows = [
        {"time": _ts(2), "security_id": "MSFT", "close": 20.0},
        {"time": _ts(1), "security_id": "AAPL", "close": 10.0},
    ]
    runtime = _runtime(
        tmp_path,
        rows,
        partition_by=("security_id",),
    )
    output = tmp_path / "interim" / "prices.materialized.jsonl"

    result = materialize_stream_to_path(
        runtime=runtime,
        stream_id="prices.raw",
        output=output,
    )

    assert json.loads(result.metadata.read_text(encoding="utf-8")) == {
        "rows": 2,
        "format": "jsonl",
        "encoding": "utf-8",
        "partition_by": ["security_id"],
        "ordered_by": ["security_id", "time"],
    }


def test_materialize_stream_refuses_existing_output_without_overwrite(
    tmp_path: Path,
    one_row_runtime: Runtime,
) -> None:
    output = tmp_path / "prices.jsonl"
    output.write_text("", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--overwrite"):
        materialize_stream_to_path(
            runtime=one_row_runtime,
            stream_id="prices.raw",
            output=output,
        )


def test_materialize_stream_refuses_managed_artifact_paths(
    tmp_path: Path,
    one_row_runtime: Runtime,
) -> None:
    output = tmp_path / "artifacts" / "ticks.jsonl"

    with pytest.raises(ValueError, match="outside the managed artifacts root"):
        materialize_stream_to_path(
            runtime=one_row_runtime,
            stream_id="prices.raw",
            output=output,
            overwrite=True,
        )

    assert not output.exists()


def test_materialize_stream_does_not_clobber_output_created_during_stream(
    tmp_path: Path,
    monkeypatch,
    one_row_runtime: Runtime,
) -> None:
    output = tmp_path / "prices.jsonl"

    def racing_rows(_runtime, _stream_id):
        def rows():
            output.write_text("concurrent output\n", encoding="utf-8")
            yield {"time": _ts(1), "security_id": "AAPL", "close": 10.0}

        return rows()

    monkeypatch.setattr(
        "datapipeline.services.materialize.materialized_stream_rows",
        racing_rows,
    )

    with pytest.raises(FileExistsError, match="already exists"):
        materialize_stream_to_path(
            runtime=one_row_runtime,
            stream_id="prices.raw",
            output=output,
        )

    assert output.read_text(encoding="utf-8") == "concurrent output\n"
    assert not output.with_suffix(".metadata.json").exists()


def test_materialize_stream_checks_metadata_overwrite_before_writing_output(
    tmp_path: Path,
    one_row_runtime: Runtime,
) -> None:
    output = tmp_path / "prices.jsonl"
    metadata = tmp_path / "prices.metadata.json"
    metadata.write_text("existing\n", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--overwrite"):
        materialize_stream_to_path(
            runtime=one_row_runtime,
            stream_id="prices.raw",
            output=output,
        )

    assert not output.exists()
    assert metadata.read_text(encoding="utf-8") == "existing\n"
