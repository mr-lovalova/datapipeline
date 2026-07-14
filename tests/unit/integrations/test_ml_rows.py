from types import SimpleNamespace

import pytest

import datapipeline.integrations.ml.adapter as adapter_module
import datapipeline.integrations.ml.rows as rows_module
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.integrations.ml.adapter import VectorAdapter


class _BrokenAdapter:
    def stream(self, **_kwargs):
        raise ValueError("invalid vector configuration")
        yield

    def iter_rows(self, **_kwargs):
        raise ValueError("invalid row configuration")
        yield


def test_ml_row_helpers_propagate_configuration_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        rows_module,
        "VectorAdapter",
        SimpleNamespace(from_project=lambda _path: _BrokenAdapter()),
    )

    with pytest.raises(ValueError, match="invalid vector configuration"):
        list(rows_module.stream_vectors("project.yaml"))

    with pytest.raises(ValueError, match="invalid row configuration"):
        list(rows_module.iter_vector_rows("project.yaml"))


def test_vector_adapter_closes_dataset_pipeline(monkeypatch) -> None:
    closed = False

    def samples():
        nonlocal closed
        try:
            yield Sample(key=("first",), features=Vector({"value": 1}))
            yield Sample(key=("second",), features=Vector({"value": 2}))
        finally:
            closed = True

    monkeypatch.setattr(
        adapter_module,
        "run_dataset_pipeline",
        lambda *_args, **_kwargs: samples(),
    )
    adapter = VectorAdapter(
        dataset=SimpleNamespace(
            features=[object()],
            targets=[],
            sample=SimpleNamespace(cadence="1d", keys=[]),
        ),
        runtime=SimpleNamespace(
            pipeline_observer=None,
            heartbeat_interval_seconds=None,
        ),
    )

    vectors = adapter.stream()
    assert next(vectors) == (("first",), Vector({"value": 1}))
    vectors.close()

    assert closed
