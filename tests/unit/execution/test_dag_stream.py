from __future__ import annotations

from pathlib import Path

import pytest

from datapipeline.execution.dag_stream import run_stage_dag
from datapipeline.execution.nodes.spec import PipelineNode
from datapipeline.execution.stage_dag import StageDag
from datapipeline.pipeline.context import PipelineContext
from datapipeline.runtime import Runtime


class _CollectingObserver:
    def __init__(self) -> None:
        self.dag_started: list[tuple[str, int]] = []
        self.node_started: list[tuple[str, str, int]] = []
        self.node_events = []
        self.dag_events = []

    def on_dag_start(self, *, dag_name: str, node_count: int) -> None:
        self.dag_started.append((dag_name, node_count))

    def on_node_start(self, *, dag_name: str, node_name: str, stage: int) -> None:
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
            PipelineNode(name="a", run=lambda _ctx, _up: [1]),
            PipelineNode(name="b", run=lambda _ctx, up: up or ()),
            PipelineNode(name="c", run=lambda _ctx, up: up or ()),
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
            PipelineNode(name="seed", run=lambda _ctx, _up: [1, 2, 3]),
            PipelineNode(
                name="plus_one",
                run=lambda _ctx, up: (x + 1 for x in (up or ())),
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
    assert observer.dag_events[0].status == "success"
    assert observer.dag_events[0].output_items == 3


def test_run_stage_dag_propagates_error_and_marks_failure(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _explode(_ctx, up):
        for value in up or ():
            if value == 2:
                raise RuntimeError("boom")
            yield value

    dag = StageDag(
        name="error-demo",
        nodes=(
            PipelineNode(name="seed", run=lambda _ctx, _up: [1, 2, 3]),
            PipelineNode(name="explode", run=_explode),
        ),
    )

    with pytest.raises(RuntimeError, match="boom"):
        list(run_stage_dag(ctx, dag, observer=observer))

    explode_events = [
        event for event in observer.node_events if event.node_name == "explode"
    ]
    assert explode_events
    assert explode_events[-1].status == "error"
    assert observer.dag_events[-1].status == "error"


def test_run_stage_dag_tracks_empty_nodes(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    dag = StageDag(
        name="empty-demo",
        nodes=(
            PipelineNode(name="seed", run=lambda _ctx, _up: []),
            PipelineNode(name="passthrough", run=lambda _ctx, up: up or ()),
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
            PipelineNode(name="first", run=lambda _ctx, _up: [1]),
            PipelineNode(name="second", run=lambda _ctx, up: up or ()),
        ),
    )

    output = list(run_stage_dag(ctx, dag, observer=observer))
    assert output == [1]
    assert [stage for _, _, stage in observer.node_started] == [1, 0]
    assert [event.stage for event in observer.node_events] == [0, 1]
