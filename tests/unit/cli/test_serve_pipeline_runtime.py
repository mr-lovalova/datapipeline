import logging
from types import SimpleNamespace

import pytest
from datapipeline.config.split import TimeSplitConfig
from datapipeline.dag.dag import Dag
from datapipeline.dag.node import PipelineNode
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import (
    SplitRuntimeOutput,
    persist_runtime_result,
)
from datapipeline.operations.runtime.pipeline import serve_with_runtime


@pytest.fixture(autouse=True)
def _skip_dataset_identity_validation(monkeypatch):
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.validate_dataset_feature_identity",
        lambda runtime, dataset: None,
    )


def _runtime():
    return SimpleNamespace(window_bounds=None, execution_observer=None)


def _runtime_with_stream_kinds(kinds):
    class _StreamSpecs:
        def get(self, stream_id):
            return SimpleNamespace(pipeline=kinds[stream_id])

    return SimpleNamespace(
        window_bounds=None,
        execution_observer=None,
        registries=SimpleNamespace(stream_specs=_StreamSpecs()),
    )


def _dataset(*, targets=None):
    return SimpleNamespace(
        features=[object()],
        targets=list(targets or []),
        group_by="1d",
        sample_keys=[],
    )


def _preview_dataset(record_stream):
    return SimpleNamespace(
        features=[SimpleNamespace(id="price", record_stream=record_stream)],
        targets=[],
        group_by="1d",
        sample_keys=[],
    )


def _target():
    return OutputTarget(
        transport="stdout",
        format="jsonl",
        view="raw",
        encoding=None,
        destination=None,
        run="run-paths",
    )


def _fs_target(destination):
    return OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=destination,
        run=None,
    )


def _serve(runtime, dataset, target, preview_index):
    return serve_with_runtime(
        runtime=runtime,
        dataset=dataset,
        limit=None,
        target=target,
        throttle_ms=None,
        preview_index=preview_index,
        visuals="on",
    )


def _sample_preview_dag():
    return Dag(
        name="pipeline:serve",
        nodes=(
            PipelineNode(
                name="vector_assemble",
                op=lambda: iter(["vector"]),
                output="vectors",
            ),
            PipelineNode(
                name="post_process",
                op=lambda stream: (f"post:{item}" for item in stream),
                input="vectors",
                output="post_processed",
            ),
        ),
    )


def test_serve_with_runtime_reraises_keyboard_interrupt_and_marks_run_failed(
    monkeypatch,
):
    runtime = _runtime()
    dataset = _dataset()
    target = _target()

    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.resolve_window_bounds",
        lambda runtime_obj, rectangular_required: (None, None),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_full_pipeline",
        lambda *args, **kwargs: _vectors(),
    )

    def _vectors():
        raise KeyboardInterrupt()
        yield None

    result = serve_with_runtime(
        runtime=runtime,
        dataset=dataset,
        limit=None,
        target=target,
        throttle_ms=None,
        preview_index=None,
        visuals="on",
    )

    with pytest.raises(KeyboardInterrupt):
        persist_runtime_result(
            result,
            target=target,
            visuals="on",
            logger=logging.getLogger(__name__),
        )


def test_serve_with_runtime_returns_split_fanout_output(monkeypatch, tmp_path):
    runtime = SimpleNamespace(
        window_bounds=None,
        execution_observer=None,
        run=SimpleNamespace(splits=["train", "val"]),
        split=TimeSplitConfig(
            boundaries=["2021-01-01T00:00:00Z"],
            labels=["train", "val"],
        ),
    )
    dataset = _dataset()
    target = _fs_target(tmp_path / "vectors.jsonl")
    samples = [
        Sample(key="2020-01-01T00:00:00Z", features=Vector(values={"x": 1})),
        Sample(key="2022-01-01T00:00:00Z", features=Vector(values={"x": 2})),
    ]

    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.resolve_window_bounds",
        lambda runtime_obj, rectangular_required: ("start", "end"),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_full_pipeline",
        lambda *args, **kwargs: iter(samples),
    )

    result = _serve(runtime, dataset, target, preview_index=None)

    assert runtime.window_bounds == ("start", "end")
    assert len(result.outputs) == 1
    output = result.outputs[0]
    assert isinstance(output, SplitRuntimeOutput)
    assert output.targets["train"].destination == tmp_path / "vectors.train.jsonl"
    assert output.targets["val"].destination == tmp_path / "vectors.val.jsonl"
    routed = list(output.rows)
    assert [output.label_for_row(sample) for sample in routed] == ["train", "val"]


def test_preview_index_12_previews_vector_assembly(monkeypatch):
    runtime = _runtime()
    dataset = _dataset(targets=[object()])
    target = _target()
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.resolve_window_bounds",
        lambda runtime_obj, rectangular_required: ("start", "end"),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_full_dag",
        lambda *args, **kwargs: _sample_preview_dag(),
    )

    result = _serve(runtime, dataset, target, preview_index=12)

    assert runtime.window_bounds == ("start", "end")
    assert len(result.outputs) == 1
    assert result.outputs[0].target == target
    assert list(result.outputs[0].rows) == ["vector"]


def test_preview_index_4_previews_derived_stream_records(monkeypatch):
    runtime = _runtime_with_stream_kinds({"derived.prices": "stream"})
    dataset = _preview_dataset("derived.prices")
    target = _target()
    captured = {}

    def _stream_id_pipeline(context, stream_id, node):
        captured["stream_id"] = stream_id
        captured["node"] = node
        return iter(["record"])

    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_stream_id_pipeline",
        _stream_id_pipeline,
    )

    result = _serve(runtime, dataset, target, preview_index=4)

    assert captured == {"stream_id": "derived.prices", "node": 0}
    assert len(result.outputs) == 1
    assert list(result.outputs[0].rows) == ["record"]


def test_preview_index_reports_stream_shape_mismatch_before_running(monkeypatch):
    runtime = _runtime_with_stream_kinds({"derived.prices": "stream"})
    dataset = _preview_dataset("derived.prices")

    def _stream_id_pipeline(*args, **kwargs):
        raise AssertionError("preview should fail before building streams")

    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_stream_id_pipeline",
        _stream_id_pipeline,
    )

    with pytest.raises(ValueError, match="Use preview indices 4-8"):
        _serve(runtime, dataset, _target(), preview_index=0)


def test_preview_index_13_previews_postprocess(monkeypatch):
    runtime = _runtime()
    dataset = _dataset()
    target = _target()

    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.resolve_window_bounds",
        lambda runtime_obj, rectangular_required: (None, None),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_full_dag",
        lambda *args, **kwargs: _sample_preview_dag(),
    )

    result = _serve(runtime, dataset, target, preview_index=13)

    assert list(result.outputs[0].rows) == ["post:vector"]


@pytest.mark.parametrize("preview_index", [-1, 14])
def test_preview_index_rejects_out_of_range_value(preview_index):
    with pytest.raises(ValueError, match="preview_index must be between 0 and 13"):
        _serve(_runtime(), _dataset(), _target(), preview_index=preview_index)
