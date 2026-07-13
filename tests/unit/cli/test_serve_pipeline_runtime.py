import logging
from types import SimpleNamespace

import pytest
from datapipeline.config.preview import PreviewStage
from datapipeline.config.split import TimeSplitConfig
from datapipeline.execution.pipeline import Pipeline
from datapipeline.execution.node import PipelineNode, SourceNode
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


def _runtime(streams=None):
    return SimpleNamespace(
        window_bounds=None,
        pipeline_observer=None,
        heartbeat_interval_seconds=None,
        split_labels=(),
        streams=streams or {},
    )


def _dataset(*, targets=None):
    return SimpleNamespace(
        features=[object()],
        targets=list(targets or []),
        sample=SimpleNamespace(cadence="1d", keys=[]),
    )


def _preview_dataset(record_stream):
    return SimpleNamespace(
        features=[SimpleNamespace(id="price", record_stream=record_stream)],
        targets=[],
        sample=SimpleNamespace(cadence="1d", keys=[]),
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


def _serve(
    runtime,
    dataset,
    target,
    preview: PreviewStage | None,
):
    return serve_with_runtime(
        runtime=runtime,
        dataset=dataset,
        limit=None,
        target=target,
        throttle_ms=None,
        preview=preview,
        visuals="on",
    )


def _sample_preview_pipeline():
    return Pipeline(
        name="pipeline:serve",
        nodes=(
            SourceNode(
                name="vector_assemble",
                open=lambda: iter(["vector"]),
            ),
            PipelineNode(
                name="normalize_features",
                apply=lambda stream: (f"post:{item}" for item in stream),
            ),
        ),
    )


def _record_preview_pipeline():
    return Pipeline(
        name="stream:prices",
        nodes=(
            SourceNode(
                name="open_source",
                open=lambda: iter(["source"]),
            ),
            PipelineNode(
                name="map_records",
                apply=lambda rows: (f"mapped:{row}" for row in rows),
            ),
            PipelineNode(
                name="floor_time",
                apply=lambda rows: (f"transformed:{row}" for row in rows),
            ),
            PipelineNode(
                name="order_records",
                apply=lambda rows: (f"records:{row}" for row in rows),
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
        "datapipeline.operations.runtime.pipeline.run_full_pipeline",
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
        preview=None,
        visuals="on",
    )

    with pytest.raises(KeyboardInterrupt):
        persist_runtime_result(
            result,
            target=target,
            logger=logging.getLogger(__name__),
        )


def test_serve_with_runtime_returns_split_fanout_output(monkeypatch, tmp_path):
    runtime = SimpleNamespace(
        window_bounds=None,
        pipeline_observer=None,
        heartbeat_interval_seconds=None,
        split_labels=("train", "val"),
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
        "datapipeline.operations.runtime.pipeline.run_full_pipeline",
        lambda *args, **kwargs: iter(samples),
    )

    result = _serve(runtime, dataset, target, preview=None)

    assert runtime.window_bounds == ("start", "end")
    assert len(result.outputs) == 1
    output = result.outputs[0]
    assert isinstance(output, SplitRuntimeOutput)
    assert output.targets["train"].destination == tmp_path / "vectors.train.jsonl"
    assert output.targets["val"].destination == tmp_path / "vectors.val.jsonl"
    routed = list(output.rows)
    assert [output.label_for_row(sample) for sample in routed] == ["train", "val"]


def test_samples_preview_stops_before_postprocess(monkeypatch):
    runtime = _runtime()
    dataset = _dataset(targets=[object()])
    target = _target()
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.resolve_window_bounds",
        lambda runtime_obj, rectangular_required: ("start", "end"),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_full_pipeline",
        lambda *args, **kwargs: _sample_preview_pipeline(),
    )

    result = _serve(runtime, dataset, target, preview="samples")

    assert runtime.window_bounds == ("start", "end")
    assert len(result.outputs) == 1
    assert result.outputs[0].target == target
    assert list(result.outputs[0].rows) == ["vector"]


@pytest.mark.parametrize(
    ("preview", "expected"),
    [
        ("source", "source"),
        ("mapped", "mapped:source"),
        ("records", "records:transformed:mapped:source"),
    ],
)
def test_record_previews_stop_at_the_named_stage(monkeypatch, preview, expected):
    captured = {}

    def build_pipeline(context, stream_id):
        captured["stream_id"] = stream_id
        return _record_preview_pipeline()

    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_stream_pipeline",
        build_pipeline,
    )
    result = _serve(
        _runtime({"derived.prices": object()}),
        _preview_dataset("derived.prices"),
        _target(),
        preview=preview,
    )

    assert captured == {"stream_id": "derived.prices"}
    assert list(result.outputs[0].rows) == [expected]


def test_features_preview_returns_processed_feature_records(monkeypatch):
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.run_feature_pipeline",
        lambda *args, **kwargs: iter(["feature"]),
    )

    result = _serve(
        _runtime(),
        _preview_dataset("derived.prices"),
        _target(),
        preview="features",
    )

    assert list(result.outputs[0].rows) == ["feature"]


def test_postprocess_preview_runs_postprocess(monkeypatch):
    runtime = _runtime()
    dataset = _dataset()
    target = _target()

    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.resolve_window_bounds",
        lambda runtime_obj, rectangular_required: (None, None),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_full_pipeline",
        lambda *args, **kwargs: _sample_preview_pipeline(),
    )

    result = _serve(runtime, dataset, target, preview="postprocess")

    assert list(result.outputs[0].rows) == ["post:vector"]


def test_preview_rejects_unknown_stage() -> None:
    with pytest.raises(ValueError, match="Unsupported preview stage"):
        _serve(
            _runtime(),
            _preview_dataset("prices"),
            _target(),
            preview="unknown",  # type: ignore[arg-type]
        )
