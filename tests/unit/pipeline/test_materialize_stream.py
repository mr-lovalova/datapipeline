import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest

from datapipeline.config.tasks import OperationTask
from datapipeline.domain.record import TemporalRecord
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import persist_runtime_result
from datapipeline.operations.runtime.materialize import materialize_stream_with_runtime
from datapipeline.pipelines.record.streams import open_record_stream
from datapipeline.runtime import Runtime, StreamRuntimeSpec
from datapipeline.services.bootstrap import bootstrap
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


def _write_project(tmp_path: Path) -> Path:
    for dirname in ("sources", "ingests", "streams", "tasks", "profiles"):
        (tmp_path / dirname).mkdir()
    (tmp_path / "postprocess.yaml").write_text("[]\n", encoding="utf-8")
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: test",
                "paths:",
                "  ingests: ingests",
                "  streams: streams",
                "  sources: sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: artifacts",
                "  tasks: tasks",
                "  profiles: profiles",
            ]
        ),
        encoding="utf-8",
    )
    return project_yaml


def _runtime(
    tmp_path: Path,
    rows: list[dict],
    partition_by: str | list[str] | None = None,
    feature_id_by: str | list[str] | None = None,
) -> Runtime:
    project_yaml = _write_project(tmp_path)
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=tmp_path / "artifacts")
    regs = runtime.registries
    regs.stream_specs.register("prices.raw", StreamRuntimeSpec(pipeline="ingest"))
    regs.stream_sources.register("prices.raw", _Source(rows))
    regs.mappers.register("prices.raw", _mapper)
    regs.record_operations.register("prices.raw", [])
    regs.stream_operations.register("prices.raw", [])
    regs.debug_operations.register("prices.raw", [])
    regs.partition_by.register("prices.raw", partition_by)
    regs.feature_id_by.register("prices.raw", feature_id_by)
    regs.ordered_by.register("prices.raw", None)
    regs.sort_batch_size.register("prices.raw", 2)
    return runtime


def test_materialize_stream_writes_jsonl_and_reusable_config(tmp_path: Path) -> None:
    rows = [
        {"time": _ts(2), "security_id": "MSFT", "close": 20.0},
        {"time": _ts(1), "security_id": "AAPL", "close": 10.0},
    ]
    runtime = _runtime(tmp_path, rows)
    runtime.sample_keys = ["security_id"]
    output = tmp_path / "interim" / "prices.materialized.jsonl"

    result = materialize_stream_to_path(
        runtime=runtime,
        stream_id="prices.raw",
        output=output,
        as_stream_id="prices.materialized",
    )

    assert result.count == 2
    payloads = [
        json.loads(line)
        for line in output.read_text(encoding="utf-8").splitlines()
    ]
    assert [(row["security_id"], row["close"]) for row in payloads] == [
        ("AAPL", 10.0),
        ("MSFT", 20.0),
    ]
    assert result.metadata == (
        tmp_path / "interim" / "prices.materialized.metadata.json"
    ).resolve()
    assert json.loads(result.metadata.read_text(encoding="utf-8")) == {
        "rows": 2,
        "format": "jsonl",
        "encoding": "utf-8",
        "partition_by": None,
        "feature_id_by": None,
        "ordered_by": ["time"],
    }
    assert result.source_config == (
        tmp_path / "sources" / "prices.materialized.source.yaml"
    ).resolve()
    assert result.ingest_config == (
        tmp_path / "ingests" / "prices.materialized.yaml"
    ).resolve()
    ingest_text = result.ingest_config.read_text(encoding="utf-8")
    assert "partition_by:" not in ingest_text
    assert "ordered_by:\n- time\n" in ingest_text

    reloaded = bootstrap(runtime.project_yaml)
    materialized = list(
        open_record_stream(reloaded_context(reloaded), "prices.materialized")
    )

    assert [(rec.security_id, rec.close) for rec in materialized] == [
        ("AAPL", 10.0),
        ("MSFT", 20.0),
    ]


def test_materialize_stream_preserves_explicit_partition_and_feature_id_by(
    tmp_path: Path,
) -> None:
    rows = [
        {"time": _ts(2), "security_id": "MSFT", "close": 20.0},
        {"time": _ts(1), "security_id": "AAPL", "close": 10.0},
    ]
    runtime = _runtime(
        tmp_path,
        rows,
        partition_by="security_id",
        feature_id_by=[],
    )
    output = tmp_path / "interim" / "prices.materialized.jsonl"

    result = materialize_stream_to_path(
        runtime=runtime,
        stream_id="prices.raw",
        output=output,
        as_stream_id="prices.materialized",
    )

    assert json.loads(result.metadata.read_text(encoding="utf-8")) == {
        "rows": 2,
        "format": "jsonl",
        "encoding": "utf-8",
        "partition_by": ["security_id"],
        "feature_id_by": [],
        "ordered_by": ["security_id", "time"],
    }
    ingest_text = result.ingest_config.read_text(
        encoding="utf-8"
    )
    assert "partition_by:\n- security_id\n" in ingest_text
    assert "feature_id_by: []\n" in ingest_text
    assert "ordered_by:\n- security_id\n- time\n" in ingest_text

    reloaded = bootstrap(runtime.project_yaml)
    materialized = list(
        open_record_stream(reloaded_context(reloaded), "prices.materialized")
    )

    assert [(rec.security_id, rec.close) for rec in materialized] == [
        ("AAPL", 10.0),
        ("MSFT", 20.0),
    ]


def reloaded_context(runtime: Runtime):
    from datapipeline.dag.context import PipelineContext

    return PipelineContext(runtime)


def test_materialize_stream_refuses_overwrite_without_force(tmp_path: Path) -> None:
    runtime = _runtime(
        tmp_path,
        [{"time": _ts(1), "security_id": "AAPL", "close": 10.0}],
    )
    output = tmp_path / "prices.jsonl"
    output.write_text("", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--force"):
        materialize_stream_to_path(
            runtime=runtime,
            stream_id="prices.raw",
            output=output,
        )


def test_materialize_stream_checks_config_overwrite_before_writing_output(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        [{"time": _ts(1), "security_id": "AAPL", "close": 10.0}],
    )
    existing_config = tmp_path / "sources" / "prices.materialized.source.yaml"
    existing_config.write_text("existing: true\n", encoding="utf-8")
    output = tmp_path / "prices.jsonl"

    with pytest.raises(FileExistsError, match="--force"):
        materialize_stream_to_path(
            runtime=runtime,
            stream_id="prices.raw",
            output=output,
            as_stream_id="prices.materialized",
        )

    assert not output.exists()
    assert existing_config.read_text(encoding="utf-8") == "existing: true\n"


def test_materialize_stream_checks_metadata_overwrite_before_writing_output(
    tmp_path: Path,
) -> None:
    runtime = _runtime(
        tmp_path,
        [{"time": _ts(1), "security_id": "AAPL", "close": 10.0}],
    )
    output = tmp_path / "prices.jsonl"
    metadata = tmp_path / "prices.metadata.json"
    metadata.write_text("existing\n", encoding="utf-8")

    with pytest.raises(FileExistsError, match="--force"):
        materialize_stream_to_path(
            runtime=runtime,
            stream_id="prices.raw",
            output=output,
        )

    assert not output.exists()
    assert metadata.read_text(encoding="utf-8") == "existing\n"


def test_runtime_materialize_stream_writes_metadata_sidecar(tmp_path: Path) -> None:
    runtime = _runtime(
        tmp_path,
        [
            {"time": _ts(2), "security_id": "MSFT", "close": 20.0},
            {"time": _ts(1), "security_id": "AAPL", "close": 10.0},
        ],
    )
    dataset = type("Dataset", (), {"sample_keys": ["security_id"]})()
    output = tmp_path / "runtime" / "prices.jsonl"
    target = OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=output,
    )
    operation_task = OperationTask(
        id="materialize",
        kind="runtime",
        entrypoint="core.runtime.materialize",
        options={"stream": "prices.raw"},
    )

    batch = materialize_stream_with_runtime(
        runtime=runtime,
        dataset=dataset,
        target=target,
        operation_task=operation_task,
    )
    persist_runtime_result(
        batch,
        target=target,
        visuals="off",
        logger=logging.getLogger(__name__),
        emit_message=lambda *args, **kwargs: None,
    )

    metadata = output.with_suffix(".metadata.json")
    assert json.loads(metadata.read_text(encoding="utf-8")) == {
        "rows": 2,
        "format": "jsonl",
        "encoding": "utf-8",
        "partition_by": None,
        "feature_id_by": None,
        "ordered_by": ["time"],
    }
