import logging
from pathlib import Path
from typing import Any

import pytest

from datapipeline.dag.events import DagRunEvent, NodeRunEvent
from datapipeline.dag.observer import LoggingExecutionObserver
from datapipeline.dag import runner as dag_runner
from datapipeline.dag.runner import run_stage_dag
from datapipeline.dag.node import PipelineNode
from datapipeline.dag.dag import StageDag
from datapipeline.dag.context import PipelineContext
from datapipeline.runtime import Runtime


class _CollectingObserver:
    def __init__(self) -> None:
        self.dag_started: list[tuple[str, int]] = []
        self.node_started: list[tuple[str, str, int]] = []
        self.node_events = []
        self.dag_events = []

    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        depth: int = 0,
        dag_metadata: dict[str, Any] | None = None,
    ) -> None:
        self.dag_started.append((dag_name, node_count))

    def on_node_start(
        self,
        *,
        dag_name: str,
        node_name: str,
        stage: int,
        depth: int = 0,
    ) -> None:
        self.node_started.append((dag_name, node_name, stage))

    def on_node_end(self, event) -> None:
        self.node_events.append(event)

    def on_dag_end(self, event) -> None:
        self.dag_events.append(event)


def _context(tmp_path: Path) -> PipelineContext:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    return PipelineContext(runtime)


def test_stage_dag_upto_stage_filters_nodes() -> None:
    dag = StageDag(
        name="demo",
        nodes=(
            PipelineNode(name="a", op=lambda: [1]),
            PipelineNode(name="b", op=lambda up: up or (), input="a"),
            PipelineNode(name="c", op=lambda up: up or (), input="b"),
        ),
    )

    filtered = dag.upto_stage(1)
    assert [node.name for node in filtered.nodes] == ["a", "b"]


def test_run_stage_dag_emits_node_and_dag_events(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    dag = StageDag(
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

    output = list(run_stage_dag(ctx, dag, observer=observer))
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


def test_run_stage_dag_propagates_error_and_marks_failure(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _explode(up):
        for value in up or ():
            if value == 2:
                raise RuntimeError("boom")
            yield value

    dag = StageDag(
        name="error-demo",
        nodes=(
            PipelineNode(name="seed", op=lambda: [1, 2, 3]),
            PipelineNode(name="explode", op=_explode, input="seed"),
        ),
    )

    with pytest.raises(RuntimeError, match="boom"):
        list(run_stage_dag(ctx, dag, observer=observer))

    explode_events = [
        event for event in observer.node_events if event.node_name == "explode"
    ]
    assert explode_events
    assert explode_events[-1].status == "error"
    assert explode_events[-1].error_type == "RuntimeError"
    assert explode_events[-1].depth == 1
    assert observer.dag_events[-1].status == "error"
    assert observer.dag_events[-1].error_type == "RuntimeError"
    assert observer.dag_events[-1].depth == 0


def test_run_stage_dag_keyboard_interrupt_marks_failure(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _interrupt(up):
        for value in up or ():
            if value == 2:
                raise KeyboardInterrupt()
            yield value

    dag = StageDag(
        name="interrupt-demo",
        nodes=(
            PipelineNode(name="seed", op=lambda: [1, 2, 3]),
            PipelineNode(name="interrupt", op=_interrupt, input="seed"),
        ),
    )

    with pytest.raises(KeyboardInterrupt):
        list(run_stage_dag(ctx, dag, observer=observer))

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

    interrupt_dag = StageDag(
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
        list(run_stage_dag(ctx, interrupt_dag))

    assert dag_runner._run_interrupted() is True

    success_dag = StageDag(
        name="success-demo",
        nodes=(PipelineNode(name="seed", op=lambda: [1]),),
    )
    assert list(run_stage_dag(ctx, success_dag)) == [1]
    assert dag_runner._run_interrupted() is False


def test_run_stage_dag_emits_explicit_depth_for_nested_dags(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _inner_stream():
        inner = StageDag(
            name="inner",
            nodes=(PipelineNode(name="seed_inner", op=lambda: [1, 2]),),
        )
        return run_stage_dag(ctx, inner, observer=observer)

    outer = StageDag(
        name="outer",
        nodes=(PipelineNode(name="open_inner", op=_inner_stream),),
    )

    output = list(run_stage_dag(ctx, outer, observer=observer))
    assert output == [1, 2]

    dag_by_name = {event.dag_name: event for event in observer.dag_events}
    assert dag_by_name["outer"].depth == 0
    assert dag_by_name["inner"].depth == 1
    node_depths = {event.node_name: event.depth for event in observer.node_events}
    assert node_depths["open_inner"] == 1
    assert node_depths["seed_inner"] == 2


def test_run_stage_dag_tracks_empty_nodes(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    dag = StageDag(
        name="empty-demo",
        nodes=(
            PipelineNode(name="seed", op=lambda: []),
            PipelineNode(name="passthrough", op=lambda up: up or (), input="seed"),
        ),
    )

    output = list(run_stage_dag(ctx, dag, observer=observer))
    assert output == []
    assert [event.node_name for event in observer.node_events] == ["seed", "passthrough"]
    assert [event.output_items for event in observer.node_events] == [0, 0]
    assert observer.dag_events[-1].output_items == 0


def test_run_stage_dag_derives_stage_from_index(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    dag = StageDag(
        name="index-demo",
        nodes=(
            PipelineNode(name="first", op=lambda: [1]),
            PipelineNode(name="second", op=lambda up: up or (), input="first"),
        ),
    )

    output = list(run_stage_dag(ctx, dag, observer=observer))
    assert output == [1]
    assert [stage for _, _, stage in observer.node_started] == [1, 0]
    assert [event.stage for event in observer.node_events] == [0, 1]


def test_run_stage_dag_fails_on_missing_input(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    dag = StageDag(
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
        list(run_stage_dag(ctx, dag))


def test_logging_observer_logs_dag_at_info_and_nodes_at_debug(caplog) -> None:
    logger = logging.getLogger("datapipeline.dag.observer.test")
    observer = LoggingExecutionObserver(logger)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="demo", node_count=2)
        observer.on_node_start(dag_name="demo", node_name="node_a", stage=0)
        observer.on_node_end(
            NodeRunEvent(
                dag_name="demo",
                node_name="node_a",
                stage=0,
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
    assert any(message.startswith("DAG started name=demo") for message in messages)
    assert any(message.startswith("DAG finished name=demo") for message in messages)
    assert not any(message.startswith("Node started ") for message in messages)
    assert not any(message.startswith("Node finished ") for message in messages)
