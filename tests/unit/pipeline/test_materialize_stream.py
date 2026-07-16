import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from datapipeline.config.dataset.dataset import FeatureDatasetConfig, SampleConfig
from datapipeline.config.execution import ExecutionConfig
from datapipeline.domain.record import TemporalRecord
from datapipeline.runtime import Runtime, SourceRuntimeStream
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
) -> Runtime:
    runtime = Runtime(
        project_yaml=tmp_path / "project.yaml",
        artifacts_root=tmp_path / "artifacts",
        dataset=FeatureDatasetConfig(sample=SampleConfig(cadence="1h")),
        execution=ExecutionConfig(),
    )
    runtime.streams["prices.raw"] = SourceRuntimeStream(
        source=_Source(rows),
        mapper=_mapper,
        preprocess=(),
        transforms=(),
        partition_by=(),
        presorted=False,
    )
    return runtime


@pytest.fixture
def one_row_runtime(tmp_path: Path) -> Runtime:
    return _runtime(
        tmp_path,
        [{"time": _ts(1), "security_id": "AAPL", "close": 10.0}],
    )


def test_materialize_stream_writes_jsonl(tmp_path: Path) -> None:
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

    payloads = [
        json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()
    ]
    assert [(row["security_id"], row["close"]) for row in payloads] == [
        ("AAPL", 10.0),
        ("MSFT", 20.0),
    ]
    assert result == output.resolve()


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

    def racing_rows(_context, _stream_id):
        def rows():
            output.write_text("concurrent output\n", encoding="utf-8")
            yield {"time": _ts(1), "security_id": "AAPL", "close": 10.0}

        return rows()

    monkeypatch.setattr(
        "datapipeline.services.materialize.run_stream_pipeline",
        racing_rows,
    )

    with pytest.raises(FileExistsError, match="already exists"):
        materialize_stream_to_path(
            runtime=one_row_runtime,
            stream_id="prices.raw",
            output=output,
        )

    assert output.read_text(encoding="utf-8") == "concurrent output\n"
