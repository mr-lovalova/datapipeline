import logging
from pathlib import Path

import pytest

from datapipeline.cli.visuals.execution import (
    ExecutionEventFormatter,
    ExecutionMessage,
    OperationProgress,
    emit_execution_message,
    make_pipeline_observer,
    make_operation_observer,
)
from datapipeline.cli.visuals.execution_context import (
    reset_current_execution_event_handler,
    set_current_execution_event_handler,
)
from datapipeline.execution.events import (
    NodeFinished,
    NodeProgress,
    NodeStarted,
    PipelineFinished,
    PipelineProgress,
    PipelineStarted,
    PipelineSummary,
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


class _CaptureHandler:
    def __init__(self) -> None:
        self.events = []

    def __call__(self, event) -> None:
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
            PipelineProgress(
                pipeline_name="pipeline",
                output_items=10,
                elapsed_seconds=60,
            ),
            logging.INFO,
            "[pipeline] running elapsed=60s items=10",
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
            logging.DEBUG,
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
    observer = make_pipeline_observer(logger)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer(PipelineStarted(pipeline_name="stream:prices", node_count=2))
        observer(
            PipelineSummary(
                pipeline_name="stream:prices",
                summary="transport=fs.file file=prices",
            )
        )
        observer(
            PipelineFinished(
                pipeline_name="stream:prices",
                node_count=2,
                output_items=3,
                elapsed_seconds=0.02,
                status="success",
            )
        )

    assert [record.getMessage() for record in caplog.records] == [
        "[stream:prices] started nodes=2",
        "[stream:prices] transport=fs.file file=prices",
        "[stream:prices] finished status=success items=3 elapsed=0.020000s",
    ]


def test_observer_logs_stages_at_debug(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.stages")
    observer = make_pipeline_observer(logger)

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer(
            NodeStarted(
                pipeline_name="pipeline",
                node_name="load",
                node_index=0,
            )
        )
        observer(
            NodeFinished(
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


def test_observer_logs_pipeline_heartbeat_at_info(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.progress")
    observer = make_pipeline_observer(logger)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer(
            NodeProgress(
                pipeline_name="feature:close",
                node_name="order_records",
                node_index=2,
                progress=ProgressSnapshot(completed=20),
                elapsed_seconds=60,
                heartbeat=True,
            )
        )
        observer(
            PipelineProgress(
                pipeline_name="feature:close",
                output_items=15,
                elapsed_seconds=60,
            )
        )

    assert len(caplog.records) == 1
    assert caplog.records[0].getMessage() == (
        "[feature:close] running elapsed=60s items=15"
    )
    assert getattr(caplog.records[0], "dp_event_kind", None) == "execution"


def test_observer_logs_only_heartbeat_node_progress_at_debug(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.node-progress")
    observer = make_pipeline_observer(logger)

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer(
            NodeProgress(
                pipeline_name="feature:close",
                node_name="order_records",
                node_index=2,
                progress=ProgressSnapshot(completed=10),
                elapsed_seconds=1,
            )
        )
        observer(
            NodeProgress(
                pipeline_name="feature:close",
                node_name="order_records",
                node_index=2,
                progress=ProgressSnapshot(completed=20),
                elapsed_seconds=60,
                heartbeat=True,
            )
        )

    assert [record.getMessage() for record in caplog.records] == [
        "[feature:close/order_records] running elapsed=60s items=20"
    ]


def test_observer_includes_error_details_on_failure(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.error")
    observer = make_pipeline_observer(logger)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer(
            PipelineFinished(
                pipeline_name="dataset",
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
            "[dataset] finished status=error "
            "error=ValueError: No entry point 'target_mapper'"
        )
    )


def test_make_pipeline_observer_routes_to_logger_and_context_handler(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.context")
    capture = _CaptureHandler()
    token = set_current_execution_event_handler(capture)
    try:
        observer = make_pipeline_observer(logger=logger)
        with caplog.at_level(logging.INFO, logger=logger.name):
            observer(PipelineStarted(pipeline_name="dataset", node_count=3))
            observer(
                PipelineFinished(
                    pipeline_name="dataset",
                    node_count=3,
                    output_items=1,
                    elapsed_seconds=0.5,
                    status="success",
                )
            )
    finally:
        reset_current_execution_event_handler(token)

    assert [type(event) for event in capture.events] == [
        PipelineStarted,
        PipelineFinished,
    ]
    assert [record.getMessage() for record in caplog.records] == [
        "[dataset] started nodes=3",
        "[dataset] finished status=success items=1 elapsed=0.500000s",
    ]


def test_context_handler_is_resolved_when_each_event_is_emitted(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.reset")
    capture = _CaptureHandler()
    token = set_current_execution_event_handler(capture)
    try:
        observer = make_pipeline_observer(logger=logger)
        observer(PipelineStarted(pipeline_name="dataset", node_count=3))
    finally:
        reset_current_execution_event_handler(token)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer(
            PipelineFinished(
                pipeline_name="dataset",
                node_count=3,
                output_items=1,
                elapsed_seconds=0.5,
                status="success",
            )
        )

    assert [type(event) for event in capture.events] == [PipelineStarted]
    assert caplog.records[-1].getMessage().startswith("[dataset] finished")


def test_emit_execution_message_uses_context_and_logger(caplog) -> None:
    capture = _CaptureHandler()
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.message")
    token = set_current_execution_event_handler(capture)
    try:
        with caplog.at_level(logging.INFO, logger=logger.name):
            emit_execution_message("Saved 2 items", logger=logger)
    finally:
        reset_current_execution_event_handler(token)

    assert len(capture.events) == 1
    assert capture.events[0] == ExecutionMessage(message="Saved 2 items")
    assert caplog.records[-1].getMessage() == "Saved 2 items"


def test_emit_execution_message_logs_without_context_handler(caplog) -> None:
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.default")

    with caplog.at_level(logging.INFO, logger=logger.name):
        emit_execution_message("Saved 3 items", logger=logger)

    assert caplog.records[-1].getMessage() == "Saved 3 items"
    assert getattr(caplog.records[-1], "dp_event_kind", None) == "execution"


def test_operation_scope_emits_flat_lifecycle_result_and_progress(caplog) -> None:
    capture = _CaptureHandler()
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.operation")
    token = set_current_execution_event_handler(capture)
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
        reset_current_execution_event_handler(token)

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
