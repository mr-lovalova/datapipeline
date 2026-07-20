import json
import logging
from types import SimpleNamespace

import pytest

from datapipeline.artifacts.models import VectorMetadata, VectorStatsArtifact
from datapipeline.artifacts.specs import VECTOR_METADATA, VECTOR_STATS
from datapipeline.config.dataset.dataset import DatasetConfig, SampleConfig
from datapipeline.config.dataset.variable import VariableConfig
from datapipeline.config.tasks import CoverageTask, MatrixTask
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.execution.node import PipelineNode
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import persist_runtime_result
from datapipeline.operations.runtime import coverage as coverage_ops
from datapipeline.operations.runtime import matrix as matrix_ops
from datapipeline.pipelines.dataset.nodes import PostprocessPlan


def _stats() -> VectorStatsArtifact:
    return VectorStatsArtifact.model_validate(
        {
            "schema_version": 3,
            "stage": "postprocessed",
            "total_samples": 2,
            "empty_samples": 0,
            "features": {
                "bases": [
                    {
                        "id": "speed",
                        "present_samples": 2,
                        "non_null_samples": 1,
                    }
                ],
                "columns": [
                    {
                        "id": "speed",
                        "base_id": "speed",
                        "kind": "scalar",
                        "present_samples": 2,
                        "non_null_samples": 1,
                    }
                ],
            },
            "targets": {"bases": [], "columns": []},
        }
    )


def _metadata() -> VectorMetadata:
    return VectorMetadata.model_validate(
        {
            "schema_version": 2,
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


class _CoverageContext:
    def __init__(self, runtime):
        self.runtime = runtime

    def require_artifact(self, spec):
        assert spec.key == VECTOR_STATS
        return _stats()


class _MatrixContext:
    def __init__(self, runtime):
        self.runtime = runtime

    def require_artifact(self, spec):
        assert spec.key == VECTOR_METADATA
        return _metadata()

    def window_bounds(self, rectangular_required: bool):
        assert rectangular_required is True
        return None, None


def _matrix_runtime():
    return SimpleNamespace(
        dataset=DatasetConfig(
            sample=SampleConfig(cadence="1h"),
            features=[VariableConfig(id="speed", stream="stream", field="value")],
        )
    )


def _patch_matrix(monkeypatch) -> None:
    metadata = _metadata()
    monkeypatch.setattr(matrix_ops, "PipelineContext", _MatrixContext)
    monkeypatch.setattr(
        matrix_ops,
        "build_vector_pipeline",
        lambda *_args, **_kwargs: iter(
            [Sample(key="g0", features=Vector(values={"speed": 1.0}))]
        ),
    )
    monkeypatch.setattr(
        matrix_ops,
        "build_postprocess_plan",
        lambda _context: PostprocessPlan(
            feature_entries=metadata.features,
            target_entries=metadata.targets,
            nodes=(),
        ),
    )


def _persist_result(result, target: OutputTarget | None) -> None:
    persist_runtime_result(
        result,
        target=target,
        logger=logging.getLogger(__name__),
    )


def test_inspect_coverage_reads_typed_stats_artifact(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(coverage_ops, "PipelineContext", _CoverageContext)
    destination = (tmp_path / "coverage.txt").resolve()

    result = coverage_ops.run_coverage_operation(
        runtime=SimpleNamespace(),
        task=CoverageTask(id="coverage", options={"threshold": 0.8}),
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
    assert '"threshold": 0.8' in report
    assert '"below_threshold_columns": [' in report
    assert '"speed"' in report


def test_inspect_coverage_writes_one_json_report(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(coverage_ops, "PipelineContext", _CoverageContext)
    destination = (tmp_path / "coverage.jsonl").resolve()

    result = coverage_ops.run_coverage_operation(
        runtime=SimpleNamespace(),
        task=CoverageTask(id="coverage"),
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
    assert payload["stage"] == "postprocessed"
    assert payload["features"]["columns"][0]["coverage"] == 0.5


def test_inspect_matrix_writes_jsonl(monkeypatch, tmp_path) -> None:
    _patch_matrix(monkeypatch)
    destination = (tmp_path / "matrix.jsonl").resolve()

    result = matrix_ops.run_matrix_operation(
        runtime=_matrix_runtime(),
        task=MatrixTask(id="matrix", options={"max_cells": 10}),
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

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload == {
        "vector": "feature",
        "identifier": "speed",
        "group": "g0",
        "status": "present",
    }


def test_inspect_matrix_requires_output_target(monkeypatch) -> None:
    _patch_matrix(monkeypatch)
    result = matrix_ops.run_matrix_operation(
        runtime=_matrix_runtime(),
        task=MatrixTask(id="matrix"),
    )

    with pytest.raises(ValueError, match="requires profile output target"):
        _persist_result(result, None)


def test_inspect_matrix_writes_html(monkeypatch, tmp_path) -> None:
    _patch_matrix(monkeypatch)
    destination = (tmp_path / "matrix.html").resolve()
    result = matrix_ops.run_matrix_operation(
        runtime=_matrix_runtime(),
        task=MatrixTask(id="matrix"),
    )

    _persist_result(
        result,
        OutputTarget(
            transport="fs",
            format="html",
            view="flat",
            encoding=None,
            destination=destination,
        ),
    )

    assert "Availability Matrix" in destination.read_text(encoding="utf-8")


def test_assembled_matrix_does_not_postprocess(monkeypatch) -> None:
    _patch_matrix(monkeypatch)

    def fail_plan(*_args):
        raise AssertionError("assembled matrix must not build a postprocess plan")

    monkeypatch.setattr(matrix_ops, "build_postprocess_plan", fail_plan)
    matrix_ops.run_matrix_operation(
        runtime=_matrix_runtime(),
        task=MatrixTask(id="matrix", options={"stage": "assembled"}),
    )


def test_matrix_limit_caps_samples_after_postprocess(monkeypatch) -> None:
    _patch_matrix(monkeypatch)
    metadata = _metadata()
    samples = [
        Sample(key=f"g{index}", features=Vector(values={"speed": float(index)}))
        for index in range(3)
    ]
    monkeypatch.setattr(
        matrix_ops,
        "build_vector_pipeline",
        lambda *_args, **_kwargs: iter(samples),
    )

    def drop_first(items):
        iterator = iter(items)
        next(iterator)
        return iterator

    monkeypatch.setattr(
        matrix_ops,
        "build_postprocess_plan",
        lambda _context: PostprocessPlan(
            feature_entries=metadata.features,
            target_entries=metadata.targets,
            nodes=(PipelineNode(name="drop_first", apply=drop_first),),
        ),
    )

    result = matrix_ops.run_matrix_operation(
        runtime=_matrix_runtime(),
        task=MatrixTask(id="matrix"),
        limit=1,
    )

    assert result.rows is not None
    assert [row["group"] for row in result.rows] == ["g1"]


def test_postprocessed_matrix_keeps_headers_when_every_sample_is_dropped(
    monkeypatch,
    tmp_path,
) -> None:
    _patch_matrix(monkeypatch)
    metadata = _metadata()
    drop_all = PipelineNode(name="drop_all", apply=lambda _samples: iter(()))
    monkeypatch.setattr(
        matrix_ops,
        "build_postprocess_plan",
        lambda _context: PostprocessPlan(
            feature_entries=metadata.features,
            target_entries=metadata.targets,
            nodes=(drop_all,),
        ),
    )
    destination = (tmp_path / "empty-matrix.html").resolve()

    result = matrix_ops.run_matrix_operation(
        runtime=_matrix_runtime(),
        task=MatrixTask(id="matrix"),
    )
    _persist_result(
        result,
        OutputTarget(
            transport="fs",
            format="html",
            view="flat",
            encoding=None,
            destination=destination,
        ),
    )

    document = destination.read_text(encoding="utf-8")
    assert "<th scope='col'>speed</th>" in document
    assert '"rows": []' in document
