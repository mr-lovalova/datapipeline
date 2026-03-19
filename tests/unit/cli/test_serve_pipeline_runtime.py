import logging
from types import SimpleNamespace

import pytest
from datapipeline.io.output import OutputTarget
from datapipeline.io.output import served_output_message
from datapipeline.operations.persistence import persist_runtime_result
from datapipeline.operations.runtime.pipeline import serve_with_runtime


def test_serve_with_runtime_reraises_keyboard_interrupt_and_marks_run_failed(monkeypatch):
    runtime = SimpleNamespace(window_bounds=None, execution_observer=None)
    dataset = SimpleNamespace(features=[object()], targets=[], group_by="1d")
    target = OutputTarget(
        transport="stdout",
        format="jsonl",
        view="raw",
        encoding=None,
        destination=None,
        run="run-paths",
    )

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
        step=None,
        visuals="on",
    )

    with pytest.raises(KeyboardInterrupt):
        persist_runtime_result(
            result,
            target=target,
            visuals="on",
            logger=logging.getLogger(__name__),
        )


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
