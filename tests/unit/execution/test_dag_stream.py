import logging
import time
from contextvars import ContextVar
from pathlib import Path
from typing import Any

import pytest

from datapipeline.dag.events import DagParentRef, DagRunEvent, NodeExecutionEvent
from datapipeline.dag.observer import LoggingExecutionObserver
from datapipeline.dag import runner as dag_runner
from datapipeline.dag.runner import run_dag
from datapipeline.dag.node import PipelineNode
from datapipeline.dag.dag import Dag
from datapipeline.dag.context import PipelineContext
from datapipeline.runtime import Runtime


class _CollectingObserver:
    def __init__(self) -> None:
        self.dag_started: list[tuple[str, int]] = []
        self.dag_start_depths: list[int] = []
        self.dag_start_parents: list[DagParentRef | None] = []
        self.node_started: list[tuple[str, str, int]] = []
        self.node_events = []
        self.node_progress_events = []
        self.dag_events = []

    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        depth: int = 0,
        dag_metadata: dict[str, Any] | None = None,
        dag_parent: DagParentRef | None = None,
    ) -> None:
        self.dag_started.append((dag_name, node_count))
        self.dag_start_depths.append(depth)
        self.dag_start_parents.append(dag_parent)

    def on_node_start(
        self,
        *,
        dag_name: str,
        node_name: str,
        node_index: int,
        execution_index: int,
        node_kind: str = "function",
        node_calls_dag: str | None = None,
        depth: int = 0,
    ) -> None:
        self.node_started.append((dag_name, node_name, node_index))

    def on_node_end(self, event) -> None:
        self.node_events.append(event)

    def on_node_progress(self, event) -> None:
        self.node_progress_events.append(event)

    def on_dag_end(self, event) -> None:
        self.dag_events.append(event)


def _context(tmp_path: Path) -> PipelineContext:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    return PipelineContext(runtime)


def test_dag_upto_node_filters_nodes() -> None:
    dag = Dag(
        name="demo",
        nodes=(
            PipelineNode(name="a", op=lambda: [1]),
            PipelineNode(name="b", op=lambda up: up or (), input="a"),
            PipelineNode(name="c", op=lambda up: up or (), input="b"),
        ),
    )

    filtered = dag.upto_node(1)
    assert [node.name for node in filtered.nodes] == ["a", "b"]


def test_pipeline_node_validates_kind_configuration() -> None:
    with pytest.raises(ValueError, match="requires calls_dag"):
        PipelineNode(
            name="delegate",
            op=lambda: [],
            kind="dag_call",
        )

    with pytest.raises(ValueError, match="cannot set calls_dag"):
        PipelineNode(
            name="plain",
            op=lambda: [],
            kind="function",
            calls_dag="vector:assemble",
        )

    with pytest.raises(ValueError, match="cannot set child_dags"):
        PipelineNode(
            name="plain",
            op=lambda: [],
            child_dags=(Dag(name="child", nodes=()),),
        )


def test_run_dag_emits_node_and_dag_events(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    dag = Dag(
        name="linear-demo",
        nodes=(
            PipelineNode(name="seed", op=lambda: [1, 2, 3]),
            PipelineNode(
                name="plus_one",
                op=lambda up: (x + 1 for x in (up or ())),
                input="seed",
            ),
        ),
    )

    output = list(run_dag(ctx, dag, observer=observer))
    assert output == [2, 3, 4]

    assert observer.dag_started == [("linear-demo", 2)]
    assert [name for _, name, _ in observer.node_started] == ["plus_one", "seed"]
    assert [event.node_name for event in observer.node_events] == ["seed", "plus_one"]
    assert [event.status for event in observer.node_events] == ["success", "success"]
    assert [event.output_items for event in observer.node_events] == [3, 3]
    assert [event.depth for event in observer.node_events] == [1, 1]
    assert observer.dag_events[0].status == "success"
    assert observer.dag_events[0].output_items == 3
    assert observer.dag_events[0].depth == 0


def test_run_dag_emits_node_progress_with_active_node_context(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _with_progress():
        dag_runner.emit_node_progress("items=0")
        yield 1

    dag = Dag(
        name="progress-demo",
        nodes=(PipelineNode(name="produce", op=_with_progress),),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == [1]

    assert len(observer.node_progress_events) == 1
    event = observer.node_progress_events[0]
    assert event.dag_name == "progress-demo"
    assert event.node_name == "produce"
    assert event.node_index == 0
    assert event.execution_index == 0
    assert event.message == "items=0"
    assert event.depth == 1


def test_downstream_progress_after_input_read_uses_downstream_node_context(
    tmp_path: Path,
) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _consume_with_progress(up):
        for item in up or ():
            dag_runner.emit_node_progress("after input read")
            yield item

    dag = Dag(
        name="progress-attribution-demo",
        nodes=(
            PipelineNode(name="produce", op=lambda: [1]),
            PipelineNode(
                name="consume",
                op=_consume_with_progress,
                input="produce",
            ),
        ),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == [1]

    assert len(observer.node_progress_events) == 1
    event = observer.node_progress_events[0]
    assert event.dag_name == "progress-attribution-demo"
    assert event.node_name == "consume"
    assert event.node_index == 1
    assert event.message == "after input read"


def test_run_dag_emits_heartbeat_for_quiet_node(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(dag_runner, "_HEARTBEAT_INTERVAL_SECONDS", 0.01)
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _quiet_work():
        time.sleep(0.05)
        yield 1

    dag = Dag(
        name="heartbeat-demo",
        nodes=(PipelineNode(name="produce", op=_quiet_work),),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == [1]

    heartbeats = [
        event
        for event in observer.node_progress_events
        if event.message.startswith("running elapsed=")
    ]
    assert heartbeats
    assert heartbeats[0].dag_name == "heartbeat-demo"
    assert heartbeats[0].node_name == "produce"
    assert heartbeats[0].message.endswith("items=0")


def test_run_dag_heartbeat_preserves_contextvars(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(dag_runner, "_HEARTBEAT_INTERVAL_SECONDS", 0.01)
    marker: ContextVar[str | None] = ContextVar("test_heartbeat_marker", default=None)
    observed_markers: list[str | None] = []

    class ContextObserver(_CollectingObserver):
        def on_node_progress(self, event) -> None:
            super().on_node_progress(event)
            observed_markers.append(marker.get())

    observer = ContextObserver()
    ctx = _context(tmp_path)

    def _quiet_work():
        time.sleep(0.05)
        yield 1

    dag = Dag(
        name="heartbeat-context-demo",
        nodes=(PipelineNode(name="produce", op=_quiet_work),),
    )

    token = marker.set("visual-context")
    try:
        assert list(run_dag(ctx, dag, observer=observer)) == [1]
    finally:
        marker.reset(token)

    heartbeats = [
        event
        for event in observer.node_progress_events
        if event.message.startswith("running elapsed=")
    ]
    assert heartbeats
    assert "visual-context" in observed_markers


def test_run_dag_emits_heartbeat_while_node_is_yielding_items(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(dag_runner, "_HEARTBEAT_INTERVAL_SECONDS", 0.01)
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _busy_stream():
        for item in range(20):
            time.sleep(0.003)
            yield item

    dag = Dag(
        name="heartbeat-demo",
        nodes=(PipelineNode(name="produce", op=_busy_stream),),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == list(range(20))

    heartbeats = [
        event
        for event in observer.node_progress_events
        if event.message.startswith("running elapsed=")
    ]
    assert heartbeats
    assert heartbeats[-1].dag_name == "heartbeat-demo"
    assert heartbeats[-1].node_name == "produce"
    assert not heartbeats[-1].message.endswith("items=0")


def test_heartbeat_reports_upstream_leaf_while_downstream_waits_for_input(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(dag_runner, "_HEARTBEAT_INTERVAL_SECONDS", 0.01)
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _source():
        time.sleep(0.05)
        yield 1

    def _consume(up):
        for item in up or ():
            yield item

    dag = Dag(
        name="heartbeat-leaf-demo",
        nodes=(
            PipelineNode(name="source", op=_source),
            PipelineNode(name="consume", op=_consume, input="source"),
        ),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == [1]

    heartbeats = [
        event
        for event in observer.node_progress_events
        if event.message.startswith("running elapsed=")
    ]
    assert heartbeats
    assert heartbeats[0].dag_name == "heartbeat-leaf-demo"
    assert heartbeats[0].node_name == "source"


def test_heartbeat_returns_to_downstream_node_after_upstream_is_exhausted(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(dag_runner, "_HEARTBEAT_INTERVAL_SECONDS", 0.01)
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _source():
        yield 1

    def _sort_like(up):
        values = list(up or ())
        time.sleep(0.05)
        yield values[0]

    dag = Dag(
        name="heartbeat-downstream-demo",
        nodes=(
            PipelineNode(name="source", op=_source),
            PipelineNode(name="order_records", op=_sort_like, input="source"),
        ),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == [1]

    heartbeats = [
        event
        for event in observer.node_progress_events
        if event.message.startswith("running elapsed=")
    ]
    assert heartbeats
    assert heartbeats[-1].dag_name == "heartbeat-downstream-demo"
    assert heartbeats[-1].node_name == "order_records"
    assert heartbeats[-1].message.endswith("items=0")


def test_run_dag_ignores_node_progress_when_observer_does_not_support_it(
    tmp_path: Path,
) -> None:
    class MinimalObserver:
        def on_dag_start(self, **kwargs) -> None:
            pass

        def on_node_start(self, **kwargs) -> None:
            pass

        def on_node_end(self, event) -> None:
            pass

        def on_dag_end(self, event) -> None:
            pass

    ctx = _context(tmp_path)

    def _with_progress():
        dag_runner.emit_node_progress("items=0")
        yield 1

    dag = Dag(
        name="progress-demo",
        nodes=(PipelineNode(name="produce", op=_with_progress),),
    )

    assert list(run_dag(ctx, dag, observer=MinimalObserver())) == [1]


def test_run_dag_propagates_error_and_marks_failure(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _explode(up):
        for value in up or ():
            if value == 2:
                raise RuntimeError("boom")
            yield value

    dag = Dag(
        name="error-demo",
        nodes=(
            PipelineNode(name="seed", op=lambda: [1, 2, 3]),
            PipelineNode(name="explode", op=_explode, input="seed"),
        ),
    )

    with pytest.raises(RuntimeError, match="boom"):
        list(run_dag(ctx, dag, observer=observer))

    explode_events = [
        event for event in observer.node_events if event.node_name == "explode"
    ]
    assert explode_events
    assert explode_events[-1].status == "error"
    assert explode_events[-1].error_type == "RuntimeError"
    assert explode_events[-1].error_message == "boom"
    assert explode_events[-1].depth == 1
    assert observer.dag_events[-1].status == "error"
    assert observer.dag_events[-1].error_type == "RuntimeError"
    assert observer.dag_events[-1].error_message == "boom"
    assert observer.dag_events[-1].depth == 0


def test_run_dag_keyboard_interrupt_marks_failure(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _interrupt(up):
        for value in up or ():
            if value == 2:
                raise KeyboardInterrupt()
            yield value

    dag = Dag(
        name="interrupt-demo",
        nodes=(
            PipelineNode(name="seed", op=lambda: [1, 2, 3]),
            PipelineNode(name="interrupt", op=_interrupt, input="seed"),
        ),
    )

    with pytest.raises(KeyboardInterrupt):
        list(run_dag(ctx, dag, observer=observer))

    interrupt_events = [
        event for event in observer.node_events if event.node_name == "interrupt"
    ]
    assert interrupt_events
    assert interrupt_events[-1].status == "error"
    assert interrupt_events[-1].error_type == "KeyboardInterrupt"
    assert interrupt_events[-1].depth == 1
    assert observer.dag_events[-1].status == "error"
    assert observer.dag_events[-1].error_type == "KeyboardInterrupt"
    assert observer.dag_events[-1].depth == 0


def test_interrupt_state_persists_until_next_root_run(tmp_path: Path) -> None:
    ctx = _context(tmp_path)

    interrupt_dag = Dag(
        name="interrupt-demo",
        nodes=(
            PipelineNode(name="seed", op=lambda: [1, 2]),
            PipelineNode(
                name="interrupt",
                op=lambda up: (
                    value if value == 1 else (_ for _ in ()).throw(KeyboardInterrupt())
                    for value in (up or ())
                ),
                input="seed",
            ),
        ),
    )

    with pytest.raises(KeyboardInterrupt):
        list(run_dag(ctx, interrupt_dag))

    assert dag_runner._run_interrupted() is True

    success_dag = Dag(
        name="success-demo",
        nodes=(PipelineNode(name="seed", op=lambda: [1]),),
    )
    assert list(run_dag(ctx, success_dag)) == [1]
    assert dag_runner._run_interrupted() is False


def test_run_dag_emits_explicit_depth_for_nested_dags(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _inner_stream():
        inner = Dag(
            name="inner",
            nodes=(PipelineNode(name="seed_inner", op=lambda: [1, 2]),),
        )
        return run_dag(ctx, inner, observer=observer)

    outer = Dag(
        name="outer",
        nodes=(PipelineNode(name="open_inner", op=_inner_stream),),
    )

    output = list(run_dag(ctx, outer, observer=observer))
    assert output == [1, 2]

    dag_by_name = {event.dag_name: event for event in observer.dag_events}
    assert dag_by_name["outer"].depth == 0
    assert dag_by_name["inner"].depth == 1
    assert dag_by_name["outer"].parent is None
    assert dag_by_name["inner"].parent == DagParentRef(
        dag_name="outer",
        node_name="open_inner",
        node_index=0,
    )
    start_parent_by_name = {
        dag_name: parent
        for (dag_name, _), parent in zip(observer.dag_started, observer.dag_start_parents)
    }
    assert start_parent_by_name["outer"] is None
    assert start_parent_by_name["inner"] == DagParentRef(
        dag_name="outer",
        node_name="open_inner",
        node_index=0,
    )
    node_depths = {event.node_name: event.depth for event in observer.node_events}
    assert node_depths["open_inner"] == 1
    assert node_depths["seed_inner"] == 2


def test_run_dag_uses_consuming_node_depth_for_seeded_child_dag(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _ingest_stream():
        ingest = Dag(
            name="ingest",
            nodes=(PipelineNode(name="open_source", op=lambda: [1, 2]),),
        )
        return run_dag(ctx, ingest, observer=observer)

    def _feature_stream():
        feature = Dag(
            name="feature",
            nodes=(
                PipelineNode(
                    name="build_feature_stream",
                    op=lambda records: records or (),
                    input="seed",
                ),
            ),
        )
        return run_dag(ctx, feature, seed=_ingest_stream(), observer=observer)

    outer = Dag(
        name="vector",
        nodes=(PipelineNode(name="feature_fanout", op=_feature_stream),),
    )

    assert list(run_dag(ctx, outer, observer=observer)) == [1, 2]

    start_depth_by_name = {
        dag_name: depth
        for (dag_name, _), depth in zip(
            observer.dag_started,
            observer.dag_start_depths,
        )
    }
    assert start_depth_by_name == {
        "vector": 0,
        "feature": 1,
        "ingest": 2,
    }

    parent_by_name = {
        dag_name: parent
        for (dag_name, _), parent in zip(
            observer.dag_started,
            observer.dag_start_parents,
        )
    }
    assert parent_by_name["ingest"] == DagParentRef(
        dag_name="feature",
        node_name="build_feature_stream",
        node_index=0,
    )

    node_depth_by_name = {event.node_name: event.depth for event in observer.node_events}
    assert node_depth_by_name["feature_fanout"] == 1
    assert node_depth_by_name["build_feature_stream"] == 2
    assert node_depth_by_name["open_source"] == 3


def test_run_dag_tracks_empty_nodes(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    dag = Dag(
        name="empty-demo",
        nodes=(
            PipelineNode(name="seed", op=lambda: []),
            PipelineNode(name="passthrough", op=lambda up: up or (), input="seed"),
        ),
    )

    output = list(run_dag(ctx, dag, observer=observer))
    assert output == []
    assert [event.node_name for event in observer.node_events] == ["seed", "passthrough"]
    assert [event.output_items for event in observer.node_events] == [0, 0]
    assert observer.dag_events[-1].output_items == 0


def test_run_dag_uses_node_index(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    dag = Dag(
        name="index-demo",
        nodes=(
            PipelineNode(name="first", op=lambda: [1]),
            PipelineNode(name="second", op=lambda up: up or (), input="first"),
        ),
    )

    output = list(run_dag(ctx, dag, observer=observer))
    assert output == [1]
    assert [node_index for _, _, node_index in observer.node_started] == [1, 0]
    assert [event.node_index for event in observer.node_events] == [0, 1]


def test_run_dag_fails_on_missing_input(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    dag = Dag(
        name="keyed-demo",
        nodes=(
            PipelineNode(
                name="consumer",
                op=lambda _up: [],
                input="missing",
            ),
        ),
    )

    with pytest.raises(KeyError, match="missing input"):
        list(run_dag(ctx, dag))


def test_logging_observer_logs_dag_at_info_and_nodes_at_debug(caplog) -> None:
    logger = logging.getLogger("datapipeline.dag.observer.test")
    observer = LoggingExecutionObserver(logger)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="demo", node_count=2)
        observer.on_node_start(dag_name="demo", node_name="node_a", node_index=0, execution_index=0)
        observer.on_node_end(
            NodeExecutionEvent(
                dag_name="demo",
                node_name="node_a",
                node_index=0,
                execution_index=0,
                output_items=3,
                elapsed_seconds=0.01,
                status="success",
            )
        )
        observer.on_dag_end(
            DagRunEvent(
                dag_name="demo",
                node_count=2,
                output_items=3,
                elapsed_seconds=0.02,
                status="success",
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(message.startswith("[demo] started") for message in messages)
    assert any(message.startswith("[demo] finished") for message in messages)
    assert not any(message.startswith("[demo/node_a] started") for message in messages)
    assert not any(message.startswith("[demo/node_a] finished") for message in messages)


def test_logging_observer_logs_parent_context_for_nested_dag_start(caplog) -> None:
    logger = logging.getLogger("datapipeline.dag.observer.test.parent")
    observer = LoggingExecutionObserver(logger)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(
            dag_name="vector:assemble",
            node_count=2,
            dag_parent=DagParentRef(
                dag_name="pipeline:serve",
                node_name="vector_assemble",
                node_index=0,
            ),
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(message.startswith("[vector:assemble] started nodes=2") for message in messages)
