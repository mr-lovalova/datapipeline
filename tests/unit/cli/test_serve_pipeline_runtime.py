import gzip
import json
import logging
from types import SimpleNamespace

import pytest

from datapipeline.config.dataset.dataset import DatasetConfig, SampleConfig
from datapipeline.config.dataset.split import DatasetFold, TimeInterval, TimeSplitConfig
from datapipeline.config.dataset.variable import VariableConfig
from datapipeline.config.preview import PreviewStage
from datapipeline.domain.sample import Sample
from datapipeline.domain.vector import Vector
from datapipeline.execution.node import PipelineNode, SourceNode
from datapipeline.execution.pipeline import Pipeline
from datapipeline.io.output import OutputTarget
from datapipeline.operations.persistence import (
    RoutedRuntimeOutput,
    persist_runtime_result,
)
from datapipeline.operations.runtime.pipeline import run_pipeline_operation


def _runtime(streams=None):
    runtime = SimpleNamespace(
        window_bounds=None,
        pipeline_observer=None,
        observe_node_events=True,
        heartbeat_interval_seconds=None,
        output_ids=(),
        streams=streams or {},
    )
    runtime.dataset = _dataset()
    return runtime


def _dataset(
    targets: list[VariableConfig] | None = None,
    split: TimeSplitConfig | None = None,
) -> DatasetConfig:
    return DatasetConfig(
        features=[VariableConfig(id="price", stream="prices", field="value")],
        targets=[] if targets is None else targets,
        sample=SampleConfig(cadence="1d"),
        split=split,
    )


def _preview_dataset(stream: str) -> DatasetConfig:
    return DatasetConfig(
        features=[VariableConfig(id="price", stream=stream, field="value")],
        sample=SampleConfig(cadence="1d"),
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


def _fs_target(destination, compression=None):
    return OutputTarget(
        transport="fs",
        format="jsonl",
        view="raw",
        encoding="utf-8",
        destination=destination,
        compression=compression,
        run=None,
    )


def _serve(
    runtime,
    dataset,
    target,
    preview: PreviewStage | None,
):
    runtime.dataset = dataset
    return run_pipeline_operation(
        runtime=runtime,
        limit=None,
        target=target,
        throttle_ms=None,
        preview=preview,
    )


def _sample_preview_pipeline():
    return Pipeline(
        name="dataset",
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


def test_pipeline_operation_reraises_keyboard_interrupt_and_marks_run_failed(
    monkeypatch,
):
    runtime = _runtime()
    dataset = _dataset()
    runtime.dataset = dataset
    target = _target()

    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.resolve_window_bounds",
        lambda runtime_obj, rectangular_required: (None, None),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.run_dataset_pipeline",
        lambda *args, **kwargs: _vectors(),
    )

    def _vectors():
        raise KeyboardInterrupt()
        yield None

    result = run_pipeline_operation(
        runtime=runtime,
        limit=None,
        target=target,
        throttle_ms=None,
        preview=None,
    )

    with pytest.raises(KeyboardInterrupt):
        persist_runtime_result(
            result,
            target=target,
            logger=logging.getLogger(__name__),
        )


def test_pipeline_operation_returns_split_fanout_output(monkeypatch, tmp_path):
    runtime = SimpleNamespace(
        window_bounds=None,
        pipeline_observer=None,
        observe_node_events=True,
        heartbeat_interval_seconds=None,
        output_ids=("holdout.train", "holdout.validation"),
    )
    dataset = _dataset(
        split=TimeSplitConfig(
            intervals=[
                TimeInterval(id="train", until="2021-01-01T00:00:00Z"),
                TimeInterval(id="val"),
            ],
            folds=[
                DatasetFold(
                    id="holdout",
                    train=["train"],
                    validation=["val"],
                )
            ],
        )
    )
    runtime.dataset = dataset
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
        "datapipeline.operations.runtime.pipeline.run_fold_dataset_pipeline",
        lambda *args, **kwargs: iter(samples),
    )

    result = _serve(runtime, dataset, target, preview=None)

    assert runtime.window_bounds == ("start", "end")
    assert len(result.outputs) == 1
    output = result.outputs[0]
    assert isinstance(output, RoutedRuntimeOutput)
    assert (
        output.targets["holdout.train"].destination
        == tmp_path / "vectors.holdout.train.jsonl"
    )
    assert (
        output.targets["holdout.validation"].destination
        == tmp_path / "vectors.holdout.validation.jsonl"
    )
    routed = list(output.rows)
    assert [output.output_for_row(sample) for sample in routed] == [
        "holdout.train",
        "holdout.validation",
    ]


def test_samples_preview_stops_before_postprocess(monkeypatch):
    runtime = _runtime()
    dataset = _dataset(
        targets=[VariableConfig(id="target", stream="targets", field="value")]
    )
    target = _target()
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.resolve_window_bounds",
        lambda runtime_obj, rectangular_required: ("start", "end"),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_dataset_pipeline",
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
        ("input", "source"),
        ("canonical", "mapped:source"),
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


def test_variables_preview_returns_processed_variable_records(monkeypatch):
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.run_variable_pipeline",
        lambda *args, **kwargs: iter(["variable"]),
    )

    result = _serve(
        _runtime(),
        _preview_dataset("derived.prices"),
        _target(),
        preview="variables",
    )

    assert list(result.outputs[0].rows) == ["variable"]


def test_variable_preview_rejects_duplicate_resolved_destinations(
    monkeypatch,
    tmp_path,
) -> None:
    dataset = DatasetConfig(
        features=[
            VariableConfig(id="a/b", stream="first", field="value"),
            VariableConfig(id="a?b", stream="second", field="value"),
        ],
        sample=SampleConfig(cadence="1d"),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.run_variable_pipeline",
        lambda *args, **kwargs: pytest.fail(
            "preview streams must not be opened before destinations are validated"
        ),
    )

    with pytest.raises(
        ValueError,
        match=r"Preview outputs 'a/b' and 'a\?b' resolve to the same destination",
    ):
        _serve(
            _runtime(),
            dataset,
            _fs_target(tmp_path / "preview.jsonl"),
            preview="variables",
        )

    assert not list(tmp_path.iterdir())


def test_postprocess_preview_runs_postprocess(monkeypatch):
    runtime = _runtime()
    dataset = _dataset()
    target = _target()

    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.resolve_window_bounds",
        lambda runtime_obj, rectangular_required: (None, None),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_dataset_pipeline",
        lambda *args, **kwargs: _sample_preview_pipeline(),
    )

    result = _serve(runtime, dataset, target, preview="postprocess")

    assert list(result.outputs[0].rows) == ["post:vector"]


@pytest.mark.parametrize(
    ("preview", "expected", "output_id"),
    [
        ("input", "source", "derived.prices"),
        ("canonical", "mapped:source", "derived.prices"),
        ("records", "records:transformed:mapped:source", "derived.prices"),
        ("variables", "variable", "price"),
        ("samples", "vector", None),
        ("postprocess", "post:vector", None),
    ],
)
def test_all_preview_stages_write_gzip_through_the_shared_output_path(
    monkeypatch,
    tmp_path,
    preview,
    expected,
    output_id,
) -> None:
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_stream_pipeline",
        lambda *args, **kwargs: _record_preview_pipeline(),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.run_variable_pipeline",
        lambda *args, **kwargs: iter(["variable"]),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.resolve_window_bounds",
        lambda runtime_obj, rectangular_required: (None, None),
    )
    monkeypatch.setattr(
        "datapipeline.operations.runtime.pipeline.build_dataset_pipeline",
        lambda *args, **kwargs: _sample_preview_pipeline(),
    )

    target = _fs_target(tmp_path / "preview.jsonl.gz", compression="gzip")
    result = _serve(
        _runtime({"derived.prices": object()}),
        _preview_dataset("derived.prices"),
        target,
        preview=preview,
    )
    persist_runtime_result(
        result,
        target=target,
        logger=logging.getLogger(__name__),
    )

    destination = (
        target.for_output(output_id).destination
        if output_id is not None
        else target.destination
    )
    assert destination is not None
    with gzip.open(destination, "rt", encoding="utf-8") as stream:
        assert [json.loads(line) for line in stream] == [expected]


def test_preview_rejects_unknown_stage() -> None:
    with pytest.raises(ValueError, match="Unsupported preview stage"):
        _serve(
            _runtime(),
            _preview_dataset("prices"),
            _target(),
            preview="unknown",  # type: ignore[arg-type]
        )
