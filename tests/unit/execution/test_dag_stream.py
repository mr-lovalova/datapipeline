import logging
from pathlib import Path
from typing import Any

import pytest

from datapipeline.dag.events import DagParentRef, DagRunEvent, StepRunEvent
from datapipeline.dag.observer import LoggingExecutionObserver
from datapipeline.dag import runner as dag_runner
from datapipeline.dag.runner import run_stage_dag
from datapipeline.dag.node import PipelineStep
from datapipeline.dag.dag import StageDag
from datapipeline.dag.context import PipelineContext
from datapipeline.runtime import Runtime


class _CollectingObserver:
    def __init__(self) -> None:
        self.dag_started: list[tuple[str, int]] = []
        self.dag_start_parents: list[DagParentRef | None] = []
        self.step_started: list[tuple[str, str, int]] = []
        self.step_events = []
        self.dag_events = []

    def on_dag_start(
        self,
        *,
        dag_name: str,
        step_count: int,
        depth: int = 0,
        dag_metadata: dict[str, Any] | None = None,
        dag_parent: DagParentRef | None = None,
    ) -> None:
        self.dag_started.append((dag_name, step_count))
        self.dag_start_parents.append(dag_parent)

    def on_step_start(
        self,
        *,
        dag_name: str,
        step_name: str,
        step_index: int,
        step_kind: str = "function",
        step_calls_dag: str | None = None,
        depth: int = 0,
    ) -> None:
        self.step_started.append((dag_name, step_name, step_index))

    def on_step_end(self, event) -> None:
        self.step_events.append(event)

    def on_dag_end(self, event) -> None:
        self.dag_events.append(event)


def _context(tmp_path: Path) -> PipelineContext:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    return PipelineContext(runtime)


def test_stage_dag_upto_step_filters_nodes() -> None:
    dag = StageDag(
        name="demo",
        nodes=(
            PipelineStep(name="a", op=lambda: [1]),
            PipelineStep(name="b", op=lambda up: up or (), input="a"),
            PipelineStep(name="c", op=lambda up: up or (), input="b"),
        ),
    )

    filtered = dag.upto_step(1)
    assert [node.name for node in filtered.nodes] == ["a", "b"]


def test_pipeline_step_validates_kind_configuration() -> None:
    with pytest.raises(ValueError, match="requires calls_dag"):
        PipelineStep(
            name="delegate",
            op=lambda: [],
            kind="dag_call",
        )

    with pytest.raises(ValueError, match="cannot set calls_dag"):
        PipelineStep(
            name="plain",
            op=lambda: [],
            kind="function",
            calls_dag="vector:assemble",
        )


def test_run_stage_dag_emits_step_and_dag_events(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    dag = StageDag(
        name="linear-demo",
        nodes=(
            PipelineStep(name="seed", op=lambda: [1, 2, 3]),
            PipelineStep(
                name="plus_one",
                op=lambda up: (x + 1 for x in (up or ())),
                input="seed",
            ),
        ),
    )

    output = list(run_stage_dag(ctx, dag, observer=observer))
    assert output == [2, 3, 4]

    assert observer.dag_started == [("linear-demo", 2)]
    assert [name for _, name, _ in observer.step_started] == ["plus_one", "seed"]
    assert [event.step_name for event in observer.step_events] == ["seed", "plus_one"]
    assert [event.status for event in observer.step_events] == ["success", "success"]
    assert [event.output_items for event in observer.step_events] == [3, 3]
    assert [event.depth for event in observer.step_events] == [1, 1]
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
            PipelineStep(name="seed", op=lambda: [1, 2, 3]),
            PipelineStep(name="explode", op=_explode, input="seed"),
        ),
    )

    with pytest.raises(RuntimeError, match="boom"):
        list(run_stage_dag(ctx, dag, observer=observer))

    explode_events = [
        event for event in observer.step_events if event.step_name == "explode"
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
            PipelineStep(name="seed", op=lambda: [1, 2, 3]),
            PipelineStep(name="interrupt", op=_interrupt, input="seed"),
        ),
    )

    with pytest.raises(KeyboardInterrupt):
        list(run_stage_dag(ctx, dag, observer=observer))

    interrupt_events = [
        event for event in observer.step_events if event.step_name == "interrupt"
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
            PipelineStep(name="seed", op=lambda: [1, 2]),
            PipelineStep(
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
        nodes=(PipelineStep(name="seed", op=lambda: [1]),),
    )
    assert list(run_stage_dag(ctx, success_dag)) == [1]
    assert dag_runner._run_interrupted() is False


def test_run_stage_dag_emits_explicit_depth_for_nested_dags(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _inner_stream():
        inner = StageDag(
            name="inner",
            nodes=(PipelineStep(name="seed_inner", op=lambda: [1, 2]),),
        )
        return run_stage_dag(ctx, inner, observer=observer)

    outer = StageDag(
        name="outer",
        nodes=(PipelineStep(name="open_inner", op=_inner_stream),),
    )

    output = list(run_stage_dag(ctx, outer, observer=observer))
    assert output == [1, 2]

    dag_by_name = {event.dag_name: event for event in observer.dag_events}
    assert dag_by_name["outer"].depth == 0
    assert dag_by_name["inner"].depth == 1
    assert dag_by_name["outer"].parent is None
    assert dag_by_name["inner"].parent == DagParentRef(
        dag_name="outer",
        step_name="open_inner",
        step_index=0,
    )
    start_parent_by_name = {
        dag_name: parent
        for (dag_name, _), parent in zip(observer.dag_started, observer.dag_start_parents)
    }
    assert start_parent_by_name["outer"] is None
    assert start_parent_by_name["inner"] == DagParentRef(
        dag_name="outer",
        step_name="open_inner",
        step_index=0,
    )
    step_depths = {event.step_name: event.depth for event in observer.step_events}
    assert step_depths["open_inner"] == 1
    assert step_depths["seed_inner"] == 2


def test_run_stage_dag_tracks_empty_steps(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    dag = StageDag(
        name="empty-demo",
        nodes=(
            PipelineStep(name="seed", op=lambda: []),
            PipelineStep(name="passthrough", op=lambda up: up or (), input="seed"),
        ),
    )

    output = list(run_stage_dag(ctx, dag, observer=observer))
    assert output == []
    assert [event.step_name for event in observer.step_events] == ["seed", "passthrough"]
    assert [event.output_items for event in observer.step_events] == [0, 0]
    assert observer.dag_events[-1].output_items == 0


def test_run_stage_dag_uses_step_index(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    dag = StageDag(
        name="index-demo",
        nodes=(
            PipelineStep(name="first", op=lambda: [1]),
            PipelineStep(name="second", op=lambda up: up or (), input="first"),
        ),
    )

    output = list(run_stage_dag(ctx, dag, observer=observer))
    assert output == [1]
    assert [step_index for _, _, step_index in observer.step_started] == [1, 0]
    assert [event.step_index for event in observer.step_events] == [0, 1]


def test_run_stage_dag_fails_on_missing_input(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    dag = StageDag(
        name="keyed-demo",
        nodes=(
            PipelineStep(
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
        observer.on_dag_start(dag_name="demo", step_count=2)
        observer.on_step_start(dag_name="demo", step_name="node_a", step_index=0)
        observer.on_step_end(
            StepRunEvent(
                dag_name="demo",
                step_name="node_a",
                step_index=0,
                output_items=3,
                elapsed_seconds=0.01,
                status="success",
            )
        )
        observer.on_dag_end(
            DagRunEvent(
                dag_name="demo",
                step_count=2,
                output_items=3,
                elapsed_seconds=0.02,
                status="success",
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(message.startswith("DAG started name=demo") for message in messages)
    assert any(message.startswith("DAG finished name=demo") for message in messages)
    assert not any(message.startswith("Step activated ") for message in messages)
    assert not any(message.startswith("Step finished ") for message in messages)


def test_logging_observer_logs_parent_context_for_nested_dag_start(caplog) -> None:
    logger = logging.getLogger("datapipeline.dag.observer.test.parent")
    observer = LoggingExecutionObserver(logger)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(
            dag_name="vector:assemble",
            step_count=2,
            dag_parent=DagParentRef(
                dag_name="pipeline:serve",
                step_name="vector_assemble",
                step_index=0,
            ),
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        message.startswith(
            "DAG started name=vector:assemble steps=2 "
            "parent_dag=pipeline:serve parent_step=vector_assemble parent_step_index=0"
        )
        for message in messages
    )
