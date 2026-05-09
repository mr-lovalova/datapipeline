import logging

import pytest

from datapipeline.cli.visuals.execution import (
    ExecutionEventSink,
    HierarchicalExecutionObserver,
    LoggerExecutionEventSink,
    execution_scope,
    emit_execution_message,
    make_execution_observer,
)
from datapipeline.cli.visuals.execution_context import (
    current_dag_depth,
    reset_current_execution_event_sink,
    set_current_execution_event_sink,
)
from datapipeline.dag.events import DagParentRef, DagRunEvent, NodeExecutionEvent


class _CaptureSink(ExecutionEventSink):
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)


def test_hierarchical_observer_logs_all_dags_at_info(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="outer", node_count=2, depth=0)
        observer.on_dag_start(dag_name="inner", node_count=1, depth=1)
        observer.on_dag_end(
            DagRunEvent(
                dag_name="inner",
                node_count=1,
                output_items=3,
                elapsed_seconds=0.01,
                status="success",
                depth=1,
            )
        )
        observer.on_dag_end(
            DagRunEvent(
                dag_name="outer",
                node_count=2,
                output_items=3,
                elapsed_seconds=0.02,
                status="success",
                depth=0,
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(msg.startswith("DAG started name=outer") for msg in messages)
    assert any(msg.startswith("  DAG started name=inner") for msg in messages)
    assert any(msg.startswith("  DAG finished name=inner") for msg in messages)
    assert any(msg.startswith("DAG finished name=outer") for msg in messages)


def test_hierarchical_observer_logs_nested_dags_at_debug(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.nested")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer.on_dag_start(dag_name="outer", node_count=2, depth=0)
        observer.on_dag_start(dag_name="inner", node_count=1, depth=1)
        observer.on_dag_end(
            DagRunEvent(
                dag_name="inner",
                node_count=1,
                output_items=3,
                elapsed_seconds=0.01,
                status="success",
                depth=1,
            )
        )
        observer.on_dag_end(
            DagRunEvent(
                dag_name="outer",
                node_count=2,
                output_items=3,
                elapsed_seconds=0.02,
                status="success",
                depth=0,
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(msg.startswith("  DAG started name=inner") for msg in messages)
    assert any(msg.startswith("  DAG finished name=inner") for msg in messages)


def test_hierarchical_observer_logs_parent_context_for_nested_dag_start(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.parent")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(
            dag_name="vector:assemble",
            node_count=2,
            depth=1,
            dag_parent=DagParentRef(
                dag_name="pipeline:serve",
                node_name="vector_assemble",
                node_index=0,
            ),
        )

    record = caplog.records[0]
    assert record.getMessage().startswith(
        "  DAG started name=vector:assemble nodes=2 "
        "parent_dag=pipeline:serve parent_node=vector_assemble parent_node_index=0"
    )
    assert getattr(record, "dp_parent_dag", None) == "pipeline:serve"
    assert getattr(record, "dp_parent_node", None) == "vector_assemble"
    assert getattr(record, "dp_parent_node_index", None) == 0


def test_hierarchical_observer_logs_node_events_at_debug(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.nodes")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer.on_dag_start(dag_name="demo", node_count=1, depth=0)
        observer.on_node_start(dag_name="demo", node_name="n", node_index=0, execution_index=0, depth=1)
        observer.on_node_end(
            NodeExecutionEvent(
                dag_name="demo",
                node_name="n",
                node_index=0,
                execution_index=0,
                output_items=2,
                elapsed_seconds=0.01,
                status="success",
                depth=1,
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(msg.startswith("  Node execution started dag=demo") for msg in messages)
    assert any(msg.startswith("  Node execution finished dag=demo") for msg in messages)
    assert any("index=0" in msg for msg in messages)


def test_hierarchical_observer_logs_dag_call_node_metadata(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.node.dag_call")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer.on_node_start(
            dag_name="pipeline:serve",
            node_name="vector_assemble",
            node_index=0,
            execution_index=0,
            node_kind="dag_call",
            node_calls_dag="vector:assemble",
            depth=1,
        )

    record = caplog.records[0]
    assert record.getMessage().startswith(
        "  Node execution started dag=pipeline:serve node=vector_assemble "
        "index=0 execution_index=0 kind=dag_call calls=vector:assemble"
    )
    assert getattr(record, "dp_node_kind", None) == "dag_call"
    assert getattr(record, "dp_node_calls_dag", None) == "vector:assemble"


def test_hierarchical_observer_updates_context_depth():
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.depth")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    assert current_dag_depth() == 0
    observer.on_dag_start(dag_name="outer", node_count=1, depth=0)
    assert current_dag_depth() == 1
    observer.on_dag_start(dag_name="inner", node_count=1, depth=1)
    assert current_dag_depth() == 2
    observer.on_dag_end(
        DagRunEvent(
            dag_name="inner",
            node_count=1,
            output_items=0,
            elapsed_seconds=0.0,
            status="success",
            depth=1,
        )
    )
    assert current_dag_depth() == 1
    observer.on_dag_end(
        DagRunEvent(
            dag_name="outer",
            node_count=1,
            output_items=0,
            elapsed_seconds=0.0,
            status="success",
            depth=0,
        )
    )
    assert current_dag_depth() == 0


def test_hierarchical_observer_respects_explicit_depth_when_events_finish_out_of_order(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.siblings")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="vector:assemble", node_count=2, depth=0)
        observer.on_dag_start(dag_name="feature:linear_time", node_count=9, depth=1)
        observer.on_dag_start(dag_name="feature:closing_price", node_count=9, depth=1)

        observer.on_dag_end(
            DagRunEvent(
                dag_name="vector:assemble",
                node_count=2,
                output_items=23,
                elapsed_seconds=9.0,
                status="success",
                depth=0,
            )
        )
        observer.on_dag_end(
            DagRunEvent(
                dag_name="feature:closing_price",
                node_count=9,
                output_items=33,
                elapsed_seconds=8.0,
                status="success",
                depth=1,
            )
        )
        observer.on_dag_end(
            DagRunEvent(
                dag_name="feature:linear_time",
                node_count=9,
                output_items=29,
                elapsed_seconds=9.5,
                status="success",
                depth=1,
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert messages[0].startswith("DAG started name=vector:assemble")
    assert any(msg.startswith("  DAG started name=feature:linear_time") for msg in messages)
    assert any(msg.startswith("  DAG started name=feature:closing_price") for msg in messages)
    assert any(msg.startswith("DAG finished name=vector:assemble") for msg in messages)
    assert any(msg.startswith("  DAG finished name=feature:closing_price") for msg in messages)
    assert any(msg.startswith("  DAG finished name=feature:linear_time") for msg in messages)
    assert current_dag_depth() == 1


def test_hierarchical_observer_emits_structured_log_fields(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.structured")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
        observer.on_dag_end(
            DagRunEvent(
                dag_name="pipeline:serve",
                node_count=3,
                output_items=1,
                elapsed_seconds=0.5,
                status="success",
                depth=0,
            )
        )

    assert caplog.records
    first = caplog.records[0]
    assert getattr(first, "dp_event_kind", None) == "dag_start"
    assert getattr(first, "dp_dag_name", None) == "pipeline:serve"
    assert getattr(first, "dp_depth", None) == 0


def test_hierarchical_observer_emits_index_field_for_node_events(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.node.index")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer.on_dag_start(dag_name="demo", node_count=1, depth=0)
        observer.on_node_start(dag_name="demo", node_name="n", node_index=2, execution_index=0, depth=1)

    node_start = next(
        record
        for record in caplog.records
        if getattr(record, "dp_event_kind", None) == "node_start"
    )
    assert "index=2" in node_start.getMessage()
    assert "kind=function" in node_start.getMessage()
    assert getattr(node_start, "dp_stage", None) is None
    assert getattr(node_start, "dp_index", None) == 2
    assert getattr(node_start, "dp_execution_index", None) == 0
    assert getattr(node_start, "dp_node_kind", None) == "function"


def test_hierarchical_observer_includes_error_details_on_failure(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.error_type")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
        observer.on_dag_end(
            DagRunEvent(
                dag_name="pipeline:serve",
                node_count=3,
                output_items=0,
                elapsed_seconds=0.5,
                status="error",
                error_type="ValueError",
                error_message="No entry point 'target_mapper'",
                depth=0,
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        msg.startswith(
            "DAG finished name=pipeline:serve status=error "
            "error=ValueError: No entry point 'target_mapper'"
        )
        for msg in messages
    )
    dag_end_records = [
        record
        for record in caplog.records
        if getattr(record, "dp_event_kind", None) == "dag_end"
    ]
    assert dag_end_records
    assert getattr(dag_end_records[-1], "dp_error_type", None) == "ValueError"
    assert (
        getattr(dag_end_records[-1], "dp_error_message", None)
        == "No entry point 'target_mapper'"
    )


def test_make_execution_observer_accepts_single_custom_sink():
    sink = _CaptureSink()
    observer = make_execution_observer(sink=sink)

    observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
    observer.on_dag_end(
        DagRunEvent(
            dag_name="pipeline:serve",
            node_count=3,
            output_items=1,
            elapsed_seconds=0.5,
            status="success",
            depth=0,
        )
    )

    assert [event.kind for event in sink.events] == ["dag_start", "dag_end"]


def test_hierarchical_observer_emits_dag_metadata_as_info_events():
    sink = _CaptureSink()
    observer = make_execution_observer(sink=sink)

    observer.on_dag_start(
        dag_name="feature:closing_price",
        node_count=9,
        depth=1,
        dag_metadata={
            "feature.config": {
                "id": "closing_price",
                "stream": "equity.ohlcv",
                "field": "close",
            },
            "feature.transforms": "scale,sequence",
        },
    )

    assert [event.kind for event in sink.events] == [
        "dag_start",
        "dag_info",
        "dag_info",
    ]
    assert sink.events[1].info_line == (
        "feature.config: id=closing_price stream=equity.ohlcv field=close"
    )
    assert sink.events[2].info_line == "feature.transforms: scale,sequence"


def test_hierarchical_observer_formats_dag_metadata_with_context(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.metadata")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
        observer.on_dag_start(
            dag_name="feature:closing_price",
            node_count=9,
            depth=1,
            dag_metadata={
                "feature.config": {
                    "id": "closing_price",
                    "stream": "equity.ohlcv",
                    "field": "close",
                }
            },
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        msg.startswith(
            "    [feature:closing_price] feature.config: "
            "id=closing_price stream=equity.ohlcv field=close"
        )
        for msg in messages
    )


def test_hierarchical_observer_shows_nested_dag_metadata_at_debug(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.metadata.debug")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
        observer.on_dag_start(
            dag_name="feature:closing_price",
            node_count=9,
            depth=1,
            dag_metadata={
                "feature.config": {
                    "id": "closing_price",
                    "stream": "equity.ohlcv",
                    "field": "close",
                }
            },
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        msg.startswith(
            "    [feature:closing_price] feature.config: "
            "id=closing_price stream=equity.ohlcv field=close"
        )
        for msg in messages
    )


def test_make_execution_observer_fans_out_to_multiple_sinks():
    left = _CaptureSink()
    right = _CaptureSink()
    observer = make_execution_observer(sinks=[left, right])

    observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
    observer.on_dag_end(
        DagRunEvent(
            dag_name="pipeline:serve",
            node_count=3,
            output_items=1,
            elapsed_seconds=0.5,
            status="success",
            depth=0,
        )
    )

    left_kinds = [event.kind for event in left.events]
    right_kinds = [event.kind for event in right.events]
    assert left_kinds == ["dag_start", "dag_end"]
    assert right_kinds == ["dag_start", "dag_end"]


def test_make_execution_observer_rejects_sink_and_sinks():
    sink = _CaptureSink()
    with pytest.raises(ValueError, match="either 'sink' or 'sinks'"):
        make_execution_observer(sink=sink, sinks=[sink])


def test_make_execution_observer_uses_context_sink_when_present(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.context_sink")
    capture = _CaptureSink()
    token = set_current_execution_event_sink(capture)
    try:
        observer = make_execution_observer(logger=logger)
        with caplog.at_level(logging.INFO, logger=logger.name):
            observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
            observer.on_dag_end(
                DagRunEvent(
                    dag_name="pipeline:serve",
                    node_count=3,
                    output_items=1,
                    elapsed_seconds=0.5,
                    status="success",
                    depth=0,
                )
            )
        assert [event.kind for event in capture.events] == ["dag_start", "dag_end"]
        messages = [record.getMessage() for record in caplog.records]
        assert any(msg.startswith("DAG started name=pipeline:serve") for msg in messages)
        assert any(msg.startswith("DAG finished name=pipeline:serve") for msg in messages)
    finally:
        reset_current_execution_event_sink(token)


def test_make_execution_observer_logs_without_context_sink(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.logger_default")
    observer = make_execution_observer(logger=logger)
    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
        observer.on_dag_end(
            DagRunEvent(
                dag_name="pipeline:serve",
                node_count=3,
                output_items=1,
                elapsed_seconds=0.5,
                status="success",
                depth=0,
            )
        )
    messages = [record.getMessage() for record in caplog.records]
    assert any(msg.startswith("DAG started name=pipeline:serve") for msg in messages)
    assert any(msg.startswith("DAG finished name=pipeline:serve") for msg in messages)


def test_make_execution_observer_stops_context_sink_after_context_reset(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.context_reset")
    capture = _CaptureSink()
    token = set_current_execution_event_sink(capture)
    try:
        observer = make_execution_observer(logger=logger)
        observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
    finally:
        reset_current_execution_event_sink(token)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_end(
            DagRunEvent(
                dag_name="pipeline:serve",
                node_count=3,
                output_items=1,
                elapsed_seconds=0.5,
                status="success",
                depth=0,
            )
        )

    assert [event.kind for event in capture.events] == ["dag_start"]
    messages = [record.getMessage() for record in caplog.records]
    assert any(msg.startswith("DAG finished name=pipeline:serve") for msg in messages)


def test_emit_execution_message_uses_context_sink_when_available(caplog):
    capture = _CaptureSink()
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.message_context")
    token = set_current_execution_event_sink(capture)
    try:
        with caplog.at_level(logging.INFO, logger=logger.name):
            emit_execution_message("Saved 2 items: /tmp/out.jsonl", logger=logger)
    finally:
        reset_current_execution_event_sink(token)

    assert len(capture.events) == 1
    event = capture.events[0]
    assert event.kind == "message"
    assert event.message == "Saved 2 items: /tmp/out.jsonl"
    assert event.message_kind is None
    assert event.log_level == logging.INFO
    assert any(record.getMessage() == "Saved 2 items: /tmp/out.jsonl" for record in caplog.records)


def test_emit_execution_message_supports_message_kind():
    capture = _CaptureSink()
    token = set_current_execution_event_sink(capture)
    try:
        emit_execution_message(
            "Materialized schema: /tmp/schema.json",
            message_kind="materialized",
        )
    finally:
        reset_current_execution_event_sink(token)

    assert len(capture.events) == 1
    event = capture.events[0]
    assert event.kind == "message"
    assert event.message_kind == "materialized"


def test_emit_execution_message_logs_without_context_sink(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.message_default")

    with caplog.at_level(logging.INFO, logger=logger.name):
        emit_execution_message("Saved 3 items", logger=logger)

    messages = [record.getMessage() for record in caplog.records]
    assert "Saved 3 items" in messages
    assert getattr(caplog.records[-1], "dp_event_kind", None) == "message"


def test_execution_scope_applies_to_messages_and_dag_events():
    capture = _CaptureSink()
    observer = make_execution_observer(sink=capture)
    token = set_current_execution_event_sink(capture)

    try:
        with execution_scope(
            profile_kind="serve",
            profile_name="test",
            target_id="serve",
            task_id="schema",
        ):
            emit_execution_message("in scope")
            observer.on_dag_start(dag_name="vector:assemble", node_count=2, depth=0)
    finally:
        reset_current_execution_event_sink(token)

    assert len(capture.events) == 2
    for event in capture.events:
        assert event.scope_profile_kind == "serve"
        assert event.scope_profile_name == "test"
        assert event.scope_target_id == "serve"
        assert event.scope_task_id == "schema"
