import logging

import pytest

from datapipeline.cli.visuals.execution import (
    BuildDecisionMessage,
    DagFinished,
    DagInfo,
    DagStarted,
    ExecutionEventFormatter,
    ExecutionEventSink,
    ExecutionMessage,
    ExecutionScope,
    HierarchicalExecutionObserver,
    LoggerExecutionEventSink,
    NodeFinished,
    NodeProgress,
    NodeStarted,
    OperationFinished,
    OperationInfo,
    OperationProgress,
    OperationStarted,
    ProfileStarted,
    SourceInfoMessage,
    emit_build_decision,
    execution_scope,
    emit_execution_message,
    emit_profile_started,
    emit_source_info,
    make_execution_observer,
    make_operation_observer,
)
from datapipeline.cli.visuals.execution_context import (
    current_dag_depth,
    reset_current_execution_event_sink,
    set_current_execution_event_sink,
)
from datapipeline.dag.events import (
    DagParentRef,
    DagRunEvent,
    NodeExecutionEvent,
    NodeProgressEvent,
    ProgressSnapshot,
)
from datapipeline.dag.context import PipelineContext
from datapipeline.dag.dag import Dag
from datapipeline.dag.node import PipelineNode
from datapipeline.dag.runner import run_dag
from datapipeline.execution.observability import (
    emit_operation_info,
    emit_operation_progress,
    operation_observer,
    operation_scope,
)
from datapipeline.runtime import Runtime


class _CaptureSink(ExecutionEventSink):
    def __init__(self) -> None:
        self.events = []

    def emit(self, event) -> None:
        self.events.append(event)


_SCOPE = ExecutionScope(
    profile_kind="serve",
    profile_name="test",
    target_id="pipeline",
    task_id="materialize",
    item_index="1",
    item_total="2",
)
_PARENT = DagParentRef(
    dag_name="pipeline:serve",
    node_name="vector_assemble",
    node_index=3,
)


@pytest.mark.parametrize(
    ("event", "level", "message"),
    [
        (
            ExecutionMessage(message="plain", log_level=logging.WARNING, depth=1),
            logging.WARNING,
            "  plain",
        ),
        (
            ProfileStarted(command="serve", name="default", index=1, total=1),
            logging.INFO,
            "Profile: serve default (1/1)",
        ),
        (
            ProfileStarted(command="materialize", name="adv.20", index=2, total=3),
            logging.INFO,
            "Profile: materialize adv.20 (2/3)",
        ),
        (BuildDecisionMessage(message="build"), logging.INFO, "build"),
        (
            SourceInfoMessage(source_label="prices", message="inputs", depth=1),
            logging.INFO,
            "  [prices] inputs",
        ),
        (
            DagStarted(dag_name="pipeline", node_count=2, depth=1),
            logging.INFO,
            "  [pipeline] started nodes=2",
        ),
        (
            DagInfo(
                dag_name="pipeline",
                info_name="record.order",
                info_line="record.order: time",
                depth=1,
            ),
            logging.DEBUG,
            "  [pipeline] record.order: time",
        ),
        (
            DagFinished(
                dag_name="pipeline",
                node_count=2,
                status="success",
                output_items=3,
                elapsed_seconds=0.5,
                depth=1,
            ),
            logging.INFO,
            "  [pipeline] finished status=success items=3 elapsed=0.500000s",
        ),
        (
            NodeStarted(
                dag_name="pipeline",
                node_name="load",
                node_index=0,
                execution_index=4,
                depth=1,
            ),
            logging.DEBUG,
            "[pipeline/load] started index=0 execution=4 kind=function",
        ),
        (
            NodeProgress(
                dag_name="pipeline",
                node_name="load",
                node_index=0,
                execution_index=4,
                progress=ProgressSnapshot(completed=0),
                elapsed_seconds=0,
                depth=1,
            ),
            logging.INFO,
            "[pipeline/load] running elapsed=0s items=0",
        ),
        (
            NodeFinished(
                dag_name="pipeline",
                node_name="load",
                node_index=0,
                execution_index=4,
                status="success",
                output_items=3,
                elapsed_seconds=0.25,
                depth=1,
            ),
            logging.DEBUG,
            "[pipeline/load] finished index=0 execution=4 kind=function "
            "status=success items=3 elapsed=0.250000s",
        ),
        (
            OperationStarted(
                operation_name="build:schema",
                entrypoint="core.artifact.schema",
                depth=1,
            ),
            logging.INFO,
            "  [build:schema] started operation=core.artifact.schema",
        ),
        (
            OperationInfo(
                operation_name="build:schema",
                info_line="saved path=schema.json",
                depth=1,
            ),
            logging.INFO,
            "  [build:schema] saved path=schema.json",
        ),
        (
            OperationProgress(
                operation_name="build:schema",
                step="write",
                message="running",
                depth=1,
            ),
            logging.INFO,
            "  [build:schema/write] running",
        ),
        (
            OperationFinished(
                operation_name="build:schema",
                status="success",
                elapsed_seconds=0.75,
                depth=1,
            ),
            logging.INFO,
            "  [build:schema] finished status=success elapsed=0.750000s",
        ),
    ],
)
def test_typed_execution_event_formatting(event, level, message) -> None:
    assert ExecutionEventFormatter.level(event) == level
    assert ExecutionEventFormatter.message(event) == message


def test_failed_terminal_events_are_errors() -> None:
    assert (
        ExecutionEventFormatter.level(
            DagFinished(
                dag_name="pipeline",
                node_count=1,
                status="error",
                output_items=0,
                elapsed_seconds=1,
                error_type="ValueError",
            )
        )
        == logging.ERROR
    )


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
    assert any(msg.startswith("[outer] started") for msg in messages)
    assert any(msg.startswith("  [inner] started") for msg in messages)
    assert any(msg.startswith("  [inner] finished") for msg in messages)
    assert any(msg.startswith("[outer] finished") for msg in messages)


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
    assert record.getMessage().startswith("  [vector:assemble] started nodes=2")


def test_hierarchical_observer_logs_node_events_at_debug(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.nodes")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer.on_dag_start(dag_name="demo", node_count=1, depth=0)
        observer.on_node_start(
            dag_name="demo", node_name="n", node_index=0, execution_index=0, depth=1
        )
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
    assert any(msg.startswith("[demo/n] started") for msg in messages)
    assert any(msg.startswith("[demo/n] finished") for msg in messages)
    assert any("index=0" in msg for msg in messages)


def test_hierarchical_observer_logs_persistent_node_progress_at_info(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.node_progress")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_node_progress(
            NodeProgressEvent(
                dag_name="feature:close",
                node_name="order_feature_records",
                node_index=2,
                execution_index=5,
                progress=ProgressSnapshot(completed=0),
                elapsed_seconds=60,
                persistent=True,
                depth=2,
            )
        )

    record = caplog.records[0]
    assert record.getMessage().startswith(
        "  [feature:close/order_feature_records] running elapsed=60s items=0"
    )
    assert getattr(record, "dp_event_kind", None) == "execution"


def test_hierarchical_observer_does_not_log_live_node_progress(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.live_progress")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_node_progress(
            NodeProgressEvent(
                dag_name="feature:close",
                node_name="order_feature_records",
                node_index=2,
                execution_index=5,
                progress=ProgressSnapshot(completed=10),
                elapsed_seconds=1,
                depth=2,
            )
        )

    assert caplog.records == []


def test_source_info_inside_node_uses_execution_context_label(caplog, tmp_path):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.source_info")
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir()
    context = PipelineContext(
        Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    )

    def _open_source():
        emit_source_info(
            "equity.ohlcv",
            "Inputs: left=equity.left, right=equity.right",
            logger=logger,
            depth=1,
        )
        yield 1

    dag = Dag(
        name="ingest:equity.ohlcv",
        nodes=(PipelineNode(name="open_source", op=_open_source),),
    )

    with caplog.at_level(logging.INFO, logger=logger.name):
        assert list(run_dag(context, dag)) == [1]

    assert any(
        record.getMessage().startswith(
            "  [ingest:equity.ohlcv] Inputs: left=equity.left, right=equity.right"
        )
        for record in caplog.records
    )


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
        "[pipeline:serve/vector_assemble] started "
        "index=0 execution=0 kind=dag_call calls=vector:assemble"
    )


def test_hierarchical_observer_updates_context_depth():
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.depth")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    assert current_dag_depth() == 0
    observer.on_dag_start(dag_name="outer", node_count=1, depth=0)
    assert current_dag_depth() == 0
    observer.on_dag_start(dag_name="inner", node_count=1, depth=1)
    assert current_dag_depth() == 1
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


def test_hierarchical_observer_respects_explicit_depth_when_events_finish_out_of_order(
    caplog,
):
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
    assert messages[0].startswith("[vector:assemble] started")
    assert any(msg.startswith("  [feature:linear_time] started") for msg in messages)
    assert any(msg.startswith("  [feature:closing_price] started") for msg in messages)
    assert any(msg.startswith("[vector:assemble] finished") for msg in messages)
    assert any(msg.startswith("  [feature:closing_price] finished") for msg in messages)
    assert any(msg.startswith("  [feature:linear_time] finished") for msg in messages)
    assert current_dag_depth() == 1


def test_hierarchical_observer_emits_index_field_for_node_events(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.node.index")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer.on_dag_start(dag_name="demo", node_count=1, depth=0)
        observer.on_node_start(
            dag_name="demo", node_name="n", node_index=2, execution_index=0, depth=1
        )

    node_start = next(
        record for record in caplog.records if "[demo/n] started" in record.getMessage()
    )
    assert "index=2" in node_start.getMessage()
    assert "kind=function" in node_start.getMessage()


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
            "[pipeline:serve] finished status=error "
            "error=ValueError: No entry point 'target_mapper'"
        )
        for msg in messages
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

    assert [type(event) for event in sink.events] == [DagStarted, DagFinished]


def test_hierarchical_observer_emits_dag_metadata_events():
    sink = _CaptureSink()
    observer = make_execution_observer(sink=sink)

    observer.on_dag_start(
        dag_name="ingest:equity.ohlcv",
        node_count=9,
        depth=1,
        dag_metadata={
            "source.summary": {
                "transport": "fs",
                "files": 17,
                "decoder": "jsonl",
            },
            "record.order": "id_,time",
        },
    )

    assert [type(event) for event in sink.events] == [
        DagStarted,
        DagInfo,
        DagInfo,
    ]
    assert sink.events[1].info_line == (
        "source.summary: transport=fs files=17 decoder=jsonl"
    )
    assert sink.events[2].info_line == "record.order: id_,time"


def test_hierarchical_observer_hides_nested_dag_metadata_at_info(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.metadata")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
        observer.on_dag_start(
            dag_name="ingest:equity.ohlcv",
            node_count=9,
            depth=1,
            dag_metadata={
                "source.summary": {
                    "transport": "fs",
                    "files": 17,
                    "decoder": "jsonl",
                }
            },
        )

    messages = [record.getMessage() for record in caplog.records]
    assert "[pipeline:serve] started nodes=3" in messages
    assert not any("source.summary:" in msg for msg in messages)


def test_hierarchical_observer_shows_nested_source_metadata_at_info(caplog):
    logger = logging.getLogger(
        "datapipeline.cli.visuals.execution.test.source_metadata"
    )
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
        observer.on_dag_start(
            dag_name="ingest:equity.ohlcv",
            node_count=4,
            depth=2,
            dag_metadata={
                "source": {
                    "transport": "fs.glob",
                    "count": 17,
                    "first": "2010.jsonl",
                    "last": "2026.jsonl",
                }
            },
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        msg.startswith(
            "    [ingest:equity.ohlcv] source: "
            "transport=fs.glob count=17 first=2010.jsonl last=2026.jsonl"
        )
        for msg in messages
    )


def test_hierarchical_observer_shows_nested_dag_metadata_at_debug(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.metadata.debug")
    observer = HierarchicalExecutionObserver(LoggerExecutionEventSink(logger))

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer.on_dag_start(dag_name="pipeline:serve", node_count=3, depth=0)
        observer.on_dag_start(
            dag_name="ingest:equity.ohlcv",
            node_count=9,
            depth=1,
            dag_metadata={
                "source.summary": {
                    "transport": "fs",
                    "files": 17,
                    "decoder": "jsonl",
                }
            },
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        msg.startswith(
            "  [ingest:equity.ohlcv] source.summary: "
            "transport=fs files=17 decoder=jsonl"
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

    left_types = [type(event) for event in left.events]
    right_types = [type(event) for event in right.events]
    assert left_types == [DagStarted, DagFinished]
    assert right_types == [DagStarted, DagFinished]


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
        assert [type(event) for event in capture.events] == [
            DagStarted,
            DagFinished,
        ]
        messages = [record.getMessage() for record in caplog.records]
        assert any(msg.startswith("[pipeline:serve] started") for msg in messages)
        assert any(msg.startswith("[pipeline:serve] finished") for msg in messages)
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
    assert any(msg.startswith("[pipeline:serve] started") for msg in messages)
    assert any(msg.startswith("[pipeline:serve] finished") for msg in messages)


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

    assert [type(event) for event in capture.events] == [DagStarted]
    messages = [record.getMessage() for record in caplog.records]
    assert any(msg.startswith("[pipeline:serve] finished") for msg in messages)


def test_emit_execution_message_uses_context_sink_when_available(caplog):
    capture = _CaptureSink()
    logger = logging.getLogger(
        "datapipeline.cli.visuals.execution.test.message_context"
    )
    token = set_current_execution_event_sink(capture)
    try:
        with caplog.at_level(logging.INFO, logger=logger.name):
            emit_execution_message("Saved 2 items: /tmp/out.jsonl", logger=logger)
    finally:
        reset_current_execution_event_sink(token)

    assert len(capture.events) == 1
    event = capture.events[0]
    assert isinstance(event, ExecutionMessage)
    assert event.message == "Saved 2 items: /tmp/out.jsonl"
    assert event.log_level == logging.INFO
    assert any(
        record.getMessage() == "Saved 2 items: /tmp/out.jsonl"
        for record in caplog.records
    )


def test_explicit_message_emitters_create_typed_events():
    capture = _CaptureSink()
    token = set_current_execution_event_sink(capture)
    try:
        emit_profile_started("materialize", "adv.20", 2, 3)
        emit_build_decision("build decision")
    finally:
        reset_current_execution_event_sink(token)

    assert [type(event) for event in capture.events] == [
        ProfileStarted,
        BuildDecisionMessage,
    ]
    profile_started = capture.events[0]
    assert profile_started.command == "materialize"
    assert profile_started.name == "adv.20"
    assert profile_started.index == 2
    assert profile_started.total == 3


def test_emit_execution_message_logs_without_context_sink(caplog):
    logger = logging.getLogger(
        "datapipeline.cli.visuals.execution.test.message_default"
    )

    with caplog.at_level(logging.INFO, logger=logger.name):
        emit_execution_message("Saved 3 items", logger=logger)

    messages = [record.getMessage() for record in caplog.records]
    assert "Saved 3 items" in messages
    assert getattr(caplog.records[-1], "dp_event_kind", None) == "execution"


def test_operation_scope_emits_lifecycle_and_info_events(caplog):
    capture = _CaptureSink()
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.operation")
    token = set_current_execution_event_sink(capture)
    try:
        with caplog.at_level(logging.INFO, logger=logger.name):
            observer = make_operation_observer(logger)
            with operation_observer(observer):
                with operation_scope("build:model_grid", "core.artifact.ticks"):
                    assert emit_operation_info(
                        "materialized path=/tmp/model_grid.jsonl"
                    )
                    assert emit_operation_progress(
                        "write_artifact",
                        "running elapsed=1s items=3",
                    )
    finally:
        reset_current_execution_event_sink(token)

    assert [type(event) for event in capture.events] == [
        OperationStarted,
        OperationInfo,
        OperationProgress,
        OperationFinished,
    ]
    assert capture.events[0].operation_name == "build:model_grid"
    assert capture.events[0].entrypoint == "core.artifact.ticks"
    assert capture.events[1].info_line == "materialized path=/tmp/model_grid.jsonl"
    assert capture.events[2].step == "write_artifact"
    assert capture.events[2].message == "running elapsed=1s items=3"
    messages = [record.getMessage() for record in caplog.records]
    assert "[build:model_grid] started operation=core.artifact.ticks" in messages
    assert "[build:model_grid] materialized path=/tmp/model_grid.jsonl" in messages
    assert "[build:model_grid/write_artifact] running elapsed=1s items=3" in messages


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
        assert event.scope.profile_kind == "serve"
        assert event.scope.profile_name == "test"
        assert event.scope.target_id == "serve"
        assert event.scope.task_id == "schema"
