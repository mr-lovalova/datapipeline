from types import SimpleNamespace

import pytest
from pathlib import Path

from datapipeline.operations import inspect as inspect_ops
from datapipeline.services.constants import VECTOR_SCHEMA_METADATA, VECTOR_STATS


def _snapshot() -> dict:
    return {
        "schema_version": 2,
        "match_partition": "base",
        "sample_limit": 5,
        "total_vectors": 1,
        "empty_vectors": 0,
        "group_feature_status": {"t0": {"speed": 1}},
        "group_partition_status": {"t0": {"speed": 1}},
        "group_feature_sub": {},
        "group_partition_sub": {},
    }


def _metadata() -> dict:
    return {
        "schema_version": 1,
        "features": [{"id": "speed"}],
        "targets": [],
    }


def test_inspect_coverage_reads_stats_artifact(monkeypatch, capsys) -> None:
    class _Ctx:
        def __init__(self, runtime):
            self.runtime = runtime

        def require_artifact(self, spec):
            if spec.key == VECTOR_STATS:
                return _snapshot()
            assert spec.key == VECTOR_SCHEMA_METADATA
            return _metadata()

    monkeypatch.setattr(inspect_ops, "PipelineContext", _Ctx)

    inspect_ops.inspect_coverage_with_runtime(
        runtime=SimpleNamespace(),
        dataset=SimpleNamespace(),
        operation_task=SimpleNamespace(options={}),
    )

    output = capsys.readouterr().out
    assert "Vector Coverage" in output
    assert "Total vectors processed: 1" in output


def test_inspect_thresholds_reads_stats_artifact(monkeypatch, capsys) -> None:
    class _Ctx:
        def __init__(self, runtime):
            self.runtime = runtime

        def require_artifact(self, spec):
            if spec.key == VECTOR_STATS:
                return _snapshot()
            assert spec.key == VECTOR_SCHEMA_METADATA
            return _metadata()

    monkeypatch.setattr(inspect_ops, "PipelineContext", _Ctx)

    inspect_ops.inspect_thresholds_with_runtime(
        runtime=SimpleNamespace(),
        dataset=SimpleNamespace(),
        operation_task=SimpleNamespace(options={"threshold": 0.95}),
    )

    output = capsys.readouterr().out
    assert "Vector Thresholds" in output
    assert "below_features" in output


def test_load_collector_rejects_non_object_artifact(monkeypatch) -> None:
    class _Ctx:
        def __init__(self, runtime):
            self.runtime = runtime

        def require_artifact(self, spec):
            return []

    monkeypatch.setattr(inspect_ops, "PipelineContext", _Ctx)

    with pytest.raises(RuntimeError, match="Invalid vector stats artifact"):
        inspect_ops._load_collector(
            runtime=SimpleNamespace(),
            options={},
        )


def test_inspect_matrix_emits_materialized_message(monkeypatch, tmp_path: Path) -> None:
    class _Ctx:
        def __init__(self, runtime):
            self.runtime = runtime

        def require_artifact(self, spec):
            if spec.key == VECTOR_STATS:
                return _snapshot()
            assert spec.key == VECTOR_SCHEMA_METADATA
            return _metadata()

    emitted: list[tuple[str, str | None]] = []
    monkeypatch.setattr(inspect_ops, "PipelineContext", _Ctx)
    monkeypatch.setattr(
        inspect_ops,
        "export_matrix_data",
        lambda _collector: (tmp_path / "inspect" / "matrix.html"),
    )
    monkeypatch.setattr(
        inspect_ops,
        "emit_execution_message",
        lambda message, level, logger, message_kind=None: emitted.append(
            (message, message_kind)
        ),
    )

    inspect_ops.inspect_matrix_with_runtime(
        runtime=SimpleNamespace(artifacts_root=tmp_path),
        dataset=SimpleNamespace(),
        operation_task=SimpleNamespace(options={"format": "html", "quiet": True}),
    )

    assert emitted
    assert emitted[0][1] == "materialized"
    assert emitted[0][0].startswith("Materialized inspect_matrix: ")
