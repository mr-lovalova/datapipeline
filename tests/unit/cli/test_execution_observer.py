import logging
from pathlib import Path

import pytest

from datapipeline.cli.visuals.execution import (
    PipelineFinished,
    PipelineSummary,
    PipelineStarted,
    ExecutionEventFormatter,
    PipelineEventObserver,
    ExecutionEventSink,
    ExecutionMessage,
    LoggerExecutionEventSink,
    NodeFinished,
    NodeProgress,
    NodeStarted,
    OperationProgress,
    emit_execution_message,
    make_pipeline_observer,
    make_operation_observer,
)
from datapipeline.cli.visuals.execution_context import (
    reset_current_execution_event_sink,
    set_current_execution_event_sink,
)
from datapipeline.execution.events import (
    PipelineRunEvent,
    NodeExecutionEvent,
    NodeProgressEvent,
    ProgressSnapshot,
)
from datapipeline.execution.observability import (
    FileResult,
    OperationFinished,
    OperationStarted,
    emit_file_result,
    emit_operation_progress,
    operation_observer,
    operation_scope,
)


class _CaptureSink(ExecutionEventSink):
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)


@pytest.mark.parametrize(
    ("event", "level", "message"),
    [
        (
            ExecutionMessage(message="plain", log_level=logging.WARNING),
            logging.WARNING,
            "plain",
        ),
        (
            FileResult("train_0", Path("/tmp/dataset.train_0.jsonl")),
            logging.INFO,
            "train_0: /tmp/dataset.train_0.jsonl",
        ),
        (
            PipelineStarted(pipeline_name="pipeline", node_count=2),
            logging.INFO,
            "[pipeline] started nodes=2",
        ),
        (
            PipelineSummary(
                pipeline_name="pipeline",
                summary="transport=fs.file file=prices.jsonl",
            ),
            logging.INFO,
            "[pipeline] transport=fs.file file=prices.jsonl",
        ),
        (
            PipelineFinished(
                pipeline_name="pipeline",
                node_count=2,
                status="success",
                output_items=3,
                elapsed_seconds=0.5,
            ),
            logging.INFO,
            "[pipeline] finished status=success items=3 elapsed=0.500000s",
        ),
        (
            NodeStarted(pipeline_name="pipeline", node_name="load", node_index=0),
            logging.DEBUG,
            "[pipeline/load] started",
        ),
        (
            NodeProgress(
                pipeline_name="pipeline",
                node_name="load",
                node_index=0,
                progress=ProgressSnapshot(completed=0),
                elapsed_seconds=0,
            ),
            logging.INFO,
            "[pipeline/load] running elapsed=0s items=0",
        ),
        (
            NodeFinished(
                pipeline_name="pipeline",
                node_name="load",
                node_index=0,
                status="success",
                output_items=3,
                elapsed_seconds=0.25,
            ),
            logging.DEBUG,
            "[pipeline/load] finished status=success out=3 elapsed=0.250000s",
        ),
        (
            OperationProgress(
                operation_name="build:schema",
                step="write",
                message="running",
            ),
            logging.INFO,
            "Operation build:schema · write · running",
        ),
        (
            OperationStarted("serve:dataset"),
            logging.INFO,
            "Operation serve:dataset started",
        ),
        (
            OperationFinished("serve:dataset", "success", 0.25),
            logging.INFO,
            "Operation serve:dataset finished status=success elapsed=0.250000s",
        ),
        (
            OperationFinished(
                "serve:dataset",
                "error",
                0.5,
                error_type="ValueError",
                error_message="bad\ninput",
            ),
            logging.ERROR,
            "Operation serve:dataset finished status=error "
            "error=ValueError: bad\\ninput elapsed=0.500000s",
        ),
    ],
)
def test_typed_execution_event_formatting(event, level, message) -> None:
    assert ExecutionEventFormatter.level(event) == level
    assert ExecutionEventFormatter.message(event) == message


def test_failed_terminal_events_are_errors() -> None:
    event = PipelineFinished(
        pipeline_name="pipeline",
        node_count=1,
        status="error",
        output_items=0,
        elapsed_seconds=1,
        error_type="ValueError",
    )

    assert ExecutionEventFormatter.level(event) == logging.ERROR


def test_observer_logs_root_lifecycle_and_summary_at_info(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.root")
    observer = PipelineEventObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_pipeline_start("ingest:prices", 2, "transport=fs.file file=prices")
        observer.on_pipeline_end(
            PipelineRunEvent(
                pipeline_name="ingest:prices",
                node_count=2,
                output_items=3,
                elapsed_seconds=0.02,
                status="success",
            )
        )

    assert [record.getMessage() for record in caplog.records] == [
        "[ingest:prices] started nodes=2",
        "[ingest:prices] transport=fs.file file=prices",
        "[ingest:prices] finished status=success items=3 elapsed=0.020000s",
    ]


def test_observer_logs_stages_at_debug(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.stages")
    observer = PipelineEventObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer.on_node_start("pipeline", "load", 0)
        observer.on_node_end(
            NodeExecutionEvent(
                pipeline_name="pipeline",
                node_name="load",
                node_index=0,
                output_items=2,
                elapsed_seconds=0.01,
                status="success",
            )
        )

    assert [record.getMessage() for record in caplog.records] == [
        "[pipeline/load] started",
        "[pipeline/load] finished status=success out=2 elapsed=0.010000s",
    ]


def test_observer_logs_only_persistent_stage_progress(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.progress")
    observer = PipelineEventObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_node_progress(
            NodeProgressEvent(
                pipeline_name="feature:close",
                node_name="order_records",
                node_index=2,
                progress=ProgressSnapshot(completed=10),
                elapsed_seconds=1,
            )
        )
        observer.on_node_progress(
            NodeProgressEvent(
                pipeline_name="feature:close",
                node_name="order_records",
                node_index=2,
                progress=ProgressSnapshot(completed=20),
                elapsed_seconds=60,
                persistent=True,
            )
        )

    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage() == (
        "[feature:close/order_records] running elapsed=60s items=20"
    )
    assert getattr(caplog.records[0], "dp_event_kind", None) == "execution"


def test_observer_includes_error_details_on_failure(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.error")
    observer = PipelineEventObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_pipeline_end(
            PipelineRunEvent(
                pipeline_name="pipeline:serve",
                node_count=3,
                output_items=0,
                elapsed_seconds=0.5,
                status="error",
                error_type="ValueError",
                error_message="No entry point 'target_mapper'",
            )
        )

    assert (
        caplog.records[0]
        .getMessage()
        .startswith(
            "[pipeline:serve] finished status=error "
            "error=ValueError: No entry point 'target_mapper'"
        )
    )


def test_make_pipeline_observer_accepts_single_custom_sink() -> None:
    sink = _CaptureSink()
    observer = make_pipeline_observer(sink=sink)

    observer.on_pipeline_start("pipeline:serve", 3)
    observer.on_pipeline_end(PipelineRunEvent("pipeline:serve", 3, 1, 0.5, "success"))

    assert [type(event) for event in sink.events] == [PipelineStarted, PipelineFinished]


def test_make_pipeline_observer_fans_out_to_multiple_sinks() -> None:
    left = _CaptureSink()
    right = _CaptureSink()
    observer = make_pipeline_observer(sinks=[left, right])

    observer.on_pipeline_start("pipeline:serve", 3)
    observer.on_pipeline_end(PipelineRunEvent("pipeline:serve", 3, 1, 0.5, "success"))

    assert [type(event) for event in left.events] == [PipelineStarted, PipelineFinished]
    assert [type(event) for event in right.events] == [
        PipelineStarted,
        PipelineFinished,
    ]


def test_make_pipeline_observer_rejects_ambiguous_or_empty_sinks() -> None:
    sink = _CaptureSink()
    with pytest.raises(ValueError, match="either 'sink' or 'sinks'"):
        make_pipeline_observer(sink=sink, sinks=[sink])
    with pytest.raises(ValueError, match="at least one sink"):
        make_pipeline_observer(sinks=[])


def test_make_pipeline_observer_uses_context_sink_when_present(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.context")
    capture = _CaptureSink()
    token = set_current_execution_event_sink(capture)
    try:
        observer = make_pipeline_observer(logger=logger)
        with caplog.at_level(logging.INFO, logger=logger.name):
            observer.on_pipeline_start("pipeline:serve", 3)
            observer.on_pipeline_end(
                PipelineRunEvent("pipeline:serve", 3, 1, 0.5, "success")
            )
    finally:
        reset_current_execution_event_sink(token)

    assert [type(event) for event in capture.events] == [
        PipelineStarted,
        PipelineFinished,
    ]
    assert [record.getMessage() for record in caplog.records] == [
        "[pipeline:serve] started nodes=3",
        "[pipeline:serve] finished status=success items=1 elapsed=0.500000s",
    ]


def test_context_sink_is_resolved_when_each_event_is_emitted(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.reset")
    capture = _CaptureSink()
    token = set_current_execution_event_sink(capture)
    try:
        observer = make_pipeline_observer(logger=logger)
        observer.on_pipeline_start("pipeline:serve", 3)
    finally:
        reset_current_execution_event_sink(token)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_pipeline_end(
            PipelineRunEvent("pipeline:serve", 3, 1, 0.5, "success")
        )

    assert [type(event) for event in capture.events] == [PipelineStarted]
    assert caplog.records[-1].getMessage().startswith("[pipeline:serve] finished")


def test_emit_execution_message_uses_context_and_logger(caplog) -> None:
    capture = _CaptureSink()
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.message")
    token = set_current_execution_event_sink(capture)
    try:
        with caplog.at_level(logging.INFO, logger=logger.name):
            emit_execution_message("Saved 2 items", logger=logger)
    finally:
        reset_current_execution_event_sink(token)

    assert len(capture.events) == 1
    assert capture.events[0] == ExecutionMessage(message="Saved 2 items")
    assert caplog.records[-1].getMessage() == "Saved 2 items"


def test_emit_execution_message_logs_without_context_sink(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.default")

    with caplog.at_level(logging.INFO, logger=logger.name):
        emit_execution_message("Saved 3 items", logger=logger)

    assert caplog.records[-1].getMessage() == "Saved 3 items"
    assert getattr(caplog.records[-1], "dp_event_kind", None) == "execution"


def test_operation_scope_emits_flat_lifecycle_result_and_progress(caplog) -> None:
    capture = _CaptureSink()
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.operation")
    token = set_current_execution_event_sink(capture)
    try:
        with caplog.at_level(logging.INFO, logger=logger.name):
            observer = make_operation_observer(logger)
            with operation_observer(observer), operation_scope("build:model_grid"):
                assert emit_file_result("Model grid", Path("/tmp/model_grid.jsonl"))
                assert emit_operation_progress(
                    "write_artifact",
                    "running elapsed=1s items=3",
                )
    finally:
        reset_current_execution_event_sink(token)

    assert [type(event) for event in capture.events] == [
        OperationStarted,
        FileResult,
        OperationProgress,
        OperationFinished,
    ]
    assert capture.events[1].path == Path("/tmp/model_grid.jsonl")
    assert capture.events[2].step == "write_artifact"
    messages = [record.getMessage() for record in caplog.records]
    assert "Operation build:model_grid started" in messages
    assert "Model grid: /tmp/model_grid.jsonl" in messages
    assert (
        "Operation build:model_grid · write_artifact · running elapsed=1s items=3"
        in messages
    )
    assert messages[-1].startswith("Operation build:model_grid finished status=success")
