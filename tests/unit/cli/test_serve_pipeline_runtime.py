import logging
from types import SimpleNamespace

import pytest
from datapipeline.dag.dag import Dag
from datapipeline.dag.node import PipelineNode
from datapipeline.io.output import OutputTarget
from datapipeline.io.output import served_output_message
from datapipeline.operations.persistence import persist_runtime_result
from datapipeline.operations.runtime.pipeline import serve_with_runtime


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


def _split(runtime_obj, stream):
    _ = runtime_obj
    return (f"split:{item}" for item in stream)


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
            PipelineNode(
                name="split",
                op=lambda runtime_obj, stream: _split(runtime_obj, stream),
                args=(object(),),
                input="post_processed",
                output="served",
            ),
        ),
    )


def test_serve_with_runtime_reraises_keyboard_interrupt_and_marks_run_failed(monkeypatch):
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


@pytest.mark.parametrize(
    ("preview_index", "expected"),
    [
        (13, ["post:vector"]),
        (14, ["split:post:vector"]),
    ],
)
def test_late_preview_indices_preview_postprocess_and_split(
    monkeypatch,
    preview_index,
    expected,
):
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

    result = _serve(runtime, dataset, target, preview_index=preview_index)

    assert list(result.outputs[0].rows) == expected


@pytest.mark.parametrize("preview_index", [-1, 15])
def test_preview_index_rejects_out_of_range_value(preview_index):
    with pytest.raises(ValueError, match="preview_index must be between 0 and 14"):
        _serve(_runtime(), _dataset(), _target(), preview_index=preview_index)


def test_served_output_message_for_saved_destination():
    assert (
        served_output_message(
            target=SimpleNamespace(destination="/tmp/train.jsonl", transport="fs"),
            count=14,
        )
        == "Saved 14 items: /tmp/train.jsonl"
    )


def test_served_output_message_for_stdout():
    assert (
        served_output_message(
            target=SimpleNamespace(destination=None, transport="stdout"),
            count=14,
        )
        == "Streamed 14 items: stdout"
    )


def test_served_output_message_for_non_stdout_without_destination():
    assert (
        served_output_message(
            target=SimpleNamespace(destination=None, transport="memory"),
            count=14,
        )
        == "Emitted 14 items"
    )
