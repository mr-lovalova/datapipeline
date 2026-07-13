import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest
from datapipeline.artifacts.models import VectorMetadata
from datapipeline.config.tasks import CoverageTask, MatrixTask, ThresholdsTask
from datapipeline.io.output import OutputTarget
from datapipeline.operations.runtime import coverage as coverage_ops
from datapipeline.operations.runtime import matrix as matrix_ops
from datapipeline.operations.runtime import thresholds as thresholds_ops
from datapipeline.operations.persistence import persist_runtime_result
from datapipeline.operations.runtime import vector_stats_common
from datapipeline.services.constants import VECTOR_METADATA, VECTOR_STATS


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


def _metadata() -> VectorMetadata:
    return VectorMetadata.model_validate(
        {
            "schema_version": 1,
            "features": [
                {
                    "id": "speed",
                    "base_id": "speed",
                    "kind": "scalar",
                    "present_count": 1,
                    "null_count": 0,
                    "value_types": ["float"],
                }
            ],
            "targets": [],
            "counts": {"feature_vectors": 1, "target_vectors": 0},
        }
    )


class _ArtifactCtx:
    def __init__(self, runtime):
        self.runtime = runtime

    def require_artifact(self, spec):
        if spec.key == VECTOR_STATS:
            return _snapshot()
        assert spec.key == VECTOR_METADATA
        return _metadata()


class _BrokenArtifactCtx:
    def __init__(self, runtime):
        self.runtime = runtime

    def require_artifact(self, spec):
        return []


def _patch_context(monkeypatch, *, broken: bool = False) -> None:
    monkeypatch.setattr(
        vector_stats_common,
        "PipelineContext",
        _BrokenArtifactCtx if broken else _ArtifactCtx,
    )


def _persist_result(result, target: OutputTarget | None) -> None:
    persist_runtime_result(
        result,
        target=target,
        logger=logging.getLogger(__name__),
    )


def test_inspect_coverage_reads_stats_artifact(monkeypatch, tmp_path: Path) -> None:
    _patch_context(monkeypatch)
    destination = (tmp_path / "inspect" / "coverage.txt").resolve()

    result = coverage_ops.inspect_coverage_with_runtime(
        runtime=SimpleNamespace(),
        dataset=SimpleNamespace(),
        operation_task=CoverageTask(id="coverage"),
    )
    _persist_result(
        result,
        OutputTarget(
            transport="fs",
            format="txt",
            view="flat",
            encoding="utf-8",
            destination=destination,
        ),
    )
    report = destination.read_text(encoding="utf-8")
    assert '"report": "coverage"' in report
    assert '"total_vectors": 1' in report


def test_inspect_thresholds_reads_stats_artifact(monkeypatch, tmp_path: Path) -> None:
    _patch_context(monkeypatch)
    destination = (tmp_path / "inspect" / "thresholds.txt").resolve()

    result = thresholds_ops.inspect_thresholds_with_runtime(
        runtime=SimpleNamespace(),
        dataset=SimpleNamespace(),
        operation_task=ThresholdsTask(id="thresholds"),
    )
    _persist_result(
        result,
        OutputTarget(
            transport="fs",
            format="txt",
            view="flat",
            encoding="utf-8",
            destination=destination,
        ),
    )
    report = destination.read_text(encoding="utf-8")
    assert '"report": "thresholds"' in report
    assert '"below_features"' in report


def test_inspect_coverage_applies_typed_options(monkeypatch) -> None:
    collector = object()
    loaded: list[object] = []
    built: list[tuple[object, str, float]] = []

    def load_collector(runtime):
        loaded.append(runtime)
        return collector

    def build_metrics(value, *, sort_key, threshold):
        built.append((value, sort_key, threshold))
        return {"result": "ok"}

    monkeypatch.setattr(coverage_ops, "load_collector", load_collector)
    monkeypatch.setattr(coverage_ops, "build_metrics", build_metrics)
    runtime = SimpleNamespace()

    result = coverage_ops.inspect_coverage_with_runtime(
        runtime=runtime,
        dataset=SimpleNamespace(),
        operation_task=CoverageTask(
            id="coverage",
            options={"sort": "nulls", "threshold": 0.8},
        ),
    )

    assert loaded == [runtime]
    assert built == [(collector, "nulls", 0.8)]
    assert result.payload == {
        "report": "coverage",
        "metrics": {"result": "ok"},
        "sort": "nulls",
    }


def test_inspect_thresholds_applies_typed_options(monkeypatch) -> None:
    collector = object()
    loaded: list[object] = []
    built: list[tuple[object, str, float]] = []

    def load_collector(runtime):
        loaded.append(runtime)
        return collector

    def build_metrics(value, *, sort_key, threshold):
        built.append((value, sort_key, threshold))
        return {"result": "ok"}

    monkeypatch.setattr(thresholds_ops, "load_collector", load_collector)
    monkeypatch.setattr(thresholds_ops, "build_metrics", build_metrics)
    runtime = SimpleNamespace()

    result = thresholds_ops.inspect_thresholds_with_runtime(
        runtime=runtime,
        dataset=SimpleNamespace(),
        operation_task=ThresholdsTask(
            id="thresholds",
            options={"sort": "nulls", "threshold": 0.8},
        ),
    )

    assert loaded == [runtime]
    assert built == [(collector, "nulls", 0.8)]
    assert result.payload == {
        "report": "thresholds",
        "metrics": {"result": "ok"},
        "threshold": 0.8,
    }


def test_inspect_coverage_writes_jsonl_when_fs_target(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_context(monkeypatch)
    destination = (tmp_path / "inspect" / "coverage.jsonl").resolve()

    result = coverage_ops.inspect_coverage_with_runtime(
        runtime=SimpleNamespace(),
        dataset=SimpleNamespace(),
        operation_task=CoverageTask(id="coverage"),
    )
    _persist_result(
        result,
        OutputTarget(
            transport="fs",
            format="jsonl",
            view="raw",
            encoding="utf-8",
            destination=destination,
        ),
    )

    rows = destination.read_text(encoding="utf-8").strip().splitlines()
    assert len(rows) == 1
    payload = json.loads(rows[0])
    assert payload["report"] == "coverage"
    assert payload["metrics"]["total_vectors"] == 1


def test_inspect_thresholds_writes_csv_when_fs_target(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_context(monkeypatch)
    destination = (tmp_path / "inspect" / "thresholds.csv").resolve()

    result = thresholds_ops.inspect_thresholds_with_runtime(
        runtime=SimpleNamespace(),
        dataset=SimpleNamespace(),
        operation_task=ThresholdsTask(id="thresholds"),
    )
    _persist_result(
        result,
        OutputTarget(
            transport="fs",
            format="csv",
            view="flat",
            encoding="utf-8",
            destination=destination,
        ),
    )

    csv_text = destination.read_text(encoding="utf-8")
    assert "report" in csv_text
    assert "metrics.total_vectors" in csv_text
    assert "metrics.keep_features.0" in csv_text


def test_load_collector_rejects_non_object_artifact(monkeypatch) -> None:
    _patch_context(monkeypatch, broken=True)

    with pytest.raises(RuntimeError, match="Invalid vector stats artifact"):
        vector_stats_common.load_collector(
            runtime=SimpleNamespace(),
        )


def test_inspect_matrix_writes_jsonl_when_output_target(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_context(monkeypatch)
    destination = (tmp_path / "inspect" / "matrix.jsonl").resolve()

    result = matrix_ops.inspect_matrix_with_runtime(
        runtime=SimpleNamespace(artifacts_root=tmp_path),
        dataset=SimpleNamespace(),
        operation_task=MatrixTask(id="matrix"),
    )
    _persist_result(
        result,
        OutputTarget(
            transport="fs",
            format="jsonl",
            view="raw",
            encoding="utf-8",
            destination=destination,
        ),
    )

    rows = destination.read_text(encoding="utf-8").strip().splitlines()
    assert len(rows) >= 1
    payload = json.loads(rows[0])
    assert payload["matrix_kind"] in {"feature", "partition"}
    assert "identifier" in payload


def test_inspect_matrix_requires_output_target(monkeypatch) -> None:
    _patch_context(monkeypatch)
    result = matrix_ops.inspect_matrix_with_runtime(
        runtime=SimpleNamespace(),
        dataset=SimpleNamespace(),
        operation_task=MatrixTask(id="matrix"),
    )
    with pytest.raises(ValueError, match="requires profile output target"):
        persist_runtime_result(
            result,
            target=None,
            logger=logging.getLogger(__name__),
        )


def test_inspect_matrix_writes_html_when_output_format_is_html(
    monkeypatch, tmp_path: Path
) -> None:
    _patch_context(monkeypatch)
    destination = (tmp_path / "inspect" / "matrix.html").resolve()
    written_paths: list[Path] = []

    def _export_matrix_data(collector):
        if collector.matrix_output:
            path = Path(collector.matrix_output)
            written_paths.append(path)
            return path
        return None

    monkeypatch.setattr(
        matrix_ops,
        "export_matrix_data",
        _export_matrix_data,
    )

    result = matrix_ops.inspect_matrix_with_runtime(
        runtime=SimpleNamespace(artifacts_root=tmp_path),
        dataset=SimpleNamespace(),
        operation_task=MatrixTask(id="matrix"),
    )
    persist_runtime_result(
        result,
        target=OutputTarget(
            transport="fs",
            format="html",
            view="flat",
            encoding=None,
            destination=destination,
        ),
        logger=logging.getLogger(__name__),
    )

    assert written_paths == [destination]
