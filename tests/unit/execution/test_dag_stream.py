import threading
from contextvars import ContextVar
from pathlib import Path

import pytest

from datapipeline.dag.events import (
    DagParentRef,
    DagRunEvent,
    NodeExecutionEvent,
    NodeProgressEvent,
    ProgressSnapshot,
)
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
        self.node_events: list[NodeExecutionEvent] = []
        self.node_progress_events: list[NodeProgressEvent] = []
        self.dag_events: list[DagRunEvent] = []
        self._progress_condition = threading.Condition()

    def on_dag_start(
        self,
        *,
        dag_name: str,
        node_count: int,
        depth: int = 0,
        summary: str | None = None,
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

    def on_node_end(self, event: NodeExecutionEvent) -> None:
        self.node_events.append(event)

    def on_node_progress(self, event: NodeProgressEvent) -> None:
        with self._progress_condition:
            self.node_progress_events.append(event)
            self._progress_condition.notify_all()

    def on_dag_end(self, event: DagRunEvent) -> None:
        self.dag_events.append(event)

    def wait_for_next_heartbeat(self) -> bool:
        with self._progress_condition:
            heartbeat_count = len(_heartbeat_events(self))
            return self._progress_condition.wait_for(
                lambda: len(_heartbeat_events(self)) > heartbeat_count,
                timeout=1.0,
            )


def _context(tmp_path: Path) -> PipelineContext:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    runtime = Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    return PipelineContext(runtime)


def _heartbeat_events(observer: _CollectingObserver) -> list[NodeProgressEvent]:
    return [event for event in observer.node_progress_events if event.persistent]


def test_dag_upto_node_filters_nodes() -> None:
    dag = Dag(
        name="demo",
        nodes=(
            PipelineNode(name="a", op=lambda: [1]),
            PipelineNode(name="b", op=lambda up: up, input="a"),
            PipelineNode(name="c", op=lambda up: up, input="b"),
        ),
    )

    filtered = dag.upto_node(1)
    assert [node.name for node in filtered.nodes] == ["a", "b"]


def test_pipeline_node_validates_kind_configuration() -> None:
    with pytest.raises(ValueError, match="unsupported kind"):
        PipelineNode(name="fanout", op=lambda: (), kind="dag_fanout")

    with pytest.raises(ValueError, match="requires calls_dag"):
        PipelineNode(name="delegate", op=lambda: (), kind="dag_call")

    with pytest.raises(ValueError, match="cannot set calls_dag"):
        PipelineNode(name="plain", op=lambda: (), calls_dag="vector:assemble")


def test_run_dag_emits_node_and_dag_events(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _seed() -> list[int]:
        assert observer.dag_started == [("linear-demo", 2)]
        return [1, 2, 3]

    dag = Dag(
        name="linear-demo",
        nodes=(
            PipelineNode(name="seed", op=_seed),
            PipelineNode(
                name="plus_one",
                op=lambda up: [x + 1 for x in up],
                input="seed",
            ),
        ),
    )

    stream = run_dag(ctx, dag, observer=observer)
    assert observer.dag_started == []
    output = list(stream)
    assert output == [2, 3, 4]

    assert observer.dag_started == [("linear-demo", 2)]
    assert [name for _, name, _ in observer.node_started] == ["seed", "plus_one"]
    assert [node_index for _, _, node_index in observer.node_started] == [0, 1]
    assert [event.node_name for event in observer.node_events] == ["seed", "plus_one"]
    assert [event.node_index for event in observer.node_events] == [0, 1]
    assert [event.execution_index for event in observer.node_events] == [0, 1]
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
        dag_runner.report_node_progress(
            ProgressSnapshot(completed=0, phase="preparing")
        )
        yield 1

    dag = Dag(
        name="progress-demo",
        nodes=(PipelineNode(name="produce", op=_with_progress),),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == [1]

    event = next(
        event
        for event in observer.node_progress_events
        if event.progress.completed == 0 and event.progress.phase == "preparing"
    )
    assert event.dag_name == "progress-demo"
    assert event.node_name == "produce"
    assert event.node_index == 0
    assert event.execution_index == 0
    assert event.progress == ProgressSnapshot(completed=0, phase="preparing")
    assert event.persistent is False
    assert event.depth == 1


def test_downstream_progress_after_input_read_uses_downstream_node_context(
    tmp_path: Path,
) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _consume_with_progress(up):
        for item in up:
            dag_runner.report_node_progress(
                ProgressSnapshot(
                    completed=1,
                    phase="processing",
                    detail="after input read",
                )
            )
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

    attributed = [
        event
        for event in observer.node_progress_events
        if event.progress.detail == "after input read"
    ]
    assert attributed
    assert all(event.dag_name == "progress-attribution-demo" for event in attributed)
    assert all(event.node_name == "consume" for event in attributed)
    assert all(event.node_index == 1 for event in attributed)


def test_live_progress_tracks_node_output_when_heartbeat_is_disabled(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(dag_runner, "_LIVE_PROGRESS_INTERVAL_SECONDS", 0)
    observer = _CollectingObserver()
    ctx = _context(tmp_path)
    ctx.heartbeat_interval_seconds = 0

    def _work():
        dag_runner.report_node_progress(
            ProgressSnapshot(completed=0, total=2, phase="emitting")
        )
        yield 1
        yield 2

    dag = Dag(
        name="live-progress-demo",
        nodes=(PipelineNode(name="produce", op=_work),),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == [1, 2]

    live = [event for event in observer.node_progress_events if not event.persistent]
    assert [event.progress.completed for event in live] == [0, 1, 2]
    assert live[-1].progress == ProgressSnapshot(
        completed=2,
        total=2,
        phase="emitting",
    )


def test_closing_a_stream_does_not_invent_a_progress_total(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(dag_runner, "_LIVE_PROGRESS_INTERVAL_SECONDS", 0)
    observer = _CollectingObserver()
    ctx = _context(tmp_path)
    ctx.heartbeat_interval_seconds = 0
    dag = Dag(
        name="partial-progress-demo",
        nodes=(
            PipelineNode(name="produce", op=lambda: iter((1, 2, 3))),
            PipelineNode(name="forward", op=lambda records: records, input="produce"),
        ),
    )

    stream = run_dag(ctx, dag, observer=observer)
    assert next(stream) == 1
    stream.close()

    assert observer.node_progress_events
    assert all(event.progress.total is None for event in observer.node_progress_events)
    assert all(event.progress.unit == "out" for event in observer.node_progress_events)


def test_resolve_heartbeat_interval_accepts_supported_values() -> None:
    assert (
        dag_runner.resolve_heartbeat_interval_seconds(None)
        == dag_runner.DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    )
    assert dag_runner.resolve_heartbeat_interval_seconds(0) == 0
    assert (
        dag_runner.resolve_heartbeat_interval_seconds(threading.TIMEOUT_MAX)
        == threading.TIMEOUT_MAX
    )


@pytest.mark.parametrize(
    ("interval", "message"),
    (
        pytest.param(-1, "non-negative", id="negative"),
        pytest.param(float("nan"), "finite", id="nan"),
        pytest.param(float("inf"), "finite", id="positive-infinity"),
        pytest.param(float("-inf"), "finite", id="negative-infinity"),
        pytest.param(
            threading.TIMEOUT_MAX + 1,
            "must not exceed",
            id="unsupported-timeout",
        ),
    ),
)
def test_resolve_heartbeat_interval_rejects_unsupported_values(
    interval: float,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        dag_runner.resolve_heartbeat_interval_seconds(interval)


def test_run_dag_disables_heartbeat_when_interval_is_zero(
    tmp_path: Path,
) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)
    ctx.heartbeat_interval_seconds = 0

    def _quiet_work():
        yield 1

    dag = Dag(
        name="heartbeat-disabled-demo",
        nodes=(PipelineNode(name="produce", op=_quiet_work),),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == [1]
    assert _heartbeat_events(observer) == []


def test_run_dag_heartbeat_tracks_context_and_items(tmp_path: Path) -> None:
    marker: ContextVar[str | None] = ContextVar("test_heartbeat_marker", default=None)
    observed_markers: list[str | None] = []

    class ContextObserver(_CollectingObserver):
        def on_node_progress(self, event: NodeProgressEvent) -> None:
            observed_markers.append(marker.get())
            super().on_node_progress(event)

    observer = ContextObserver()
    ctx = _context(tmp_path)
    ctx.heartbeat_interval_seconds = 0.01

    def _work():
        assert observer.wait_for_next_heartbeat()
        yield 0
        assert observer.wait_for_next_heartbeat()
        yield 1

    dag = Dag(
        name="heartbeat-context-demo",
        nodes=(PipelineNode(name="produce", op=_work),),
    )

    token = marker.set("visual-context")
    try:
        assert list(run_dag(ctx, dag, observer=observer)) == [0, 1]
    finally:
        marker.reset(token)

    heartbeats = _heartbeat_events(observer)
    assert heartbeats[0].dag_name == "heartbeat-context-demo"
    assert heartbeats[0].node_name == "produce"
    assert heartbeats[0].progress.completed == 0
    assert heartbeats[-1].progress.completed == 1
    assert set(observed_markers) == {"visual-context"}


def test_run_dag_heartbeat_reports_buffered_node_work(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)
    ctx.heartbeat_interval_seconds = 0.01

    def _work():
        dag_runner.report_node_progress(
            ProgressSnapshot(
                completed=200_000,
                phase="sorting input",
                detail="spilled runs=2",
            )
        )
        assert observer.wait_for_next_heartbeat()
        dag_runner.report_node_progress(
            ProgressSnapshot(completed=0, total=1, phase="emitting")
        )
        yield 1

    dag = Dag(
        name="heartbeat-buffering-demo",
        nodes=(PipelineNode(name="order_records", op=_work),),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == [1]

    heartbeat = _heartbeat_events(observer)[0]
    assert heartbeat.node_name == "order_records"
    assert heartbeat.progress == ProgressSnapshot(
        completed=200_000,
        phase="sorting input",
        detail="spilled runs=2",
    )


def test_heartbeat_tracks_active_node_through_nested_streams(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(dag_runner, "DEFAULT_HEARTBEAT_INTERVAL_SECONDS", 0.01)
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _source():
        assert observer.wait_for_next_heartbeat()
        yield 1

    def _sort_like(up):
        values = list(up)
        assert observer.wait_for_next_heartbeat()
        yield values[0]

    dag = Dag(
        name="heartbeat-downstream-demo",
        nodes=(
            PipelineNode(name="source", op=_source),
            PipelineNode(name="order_records", op=_sort_like, input="source"),
        ),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == [1]

    heartbeats = _heartbeat_events(observer)
    assert heartbeats[0].node_name == "source"
    assert heartbeats[-1].dag_name == "heartbeat-downstream-demo"
    assert heartbeats[-1].node_name == "order_records"
    assert heartbeats[-1].progress.completed == 0


def test_heartbeat_delivery_finishes_before_terminal_events(
    tmp_path: Path,
    monkeypatch,
) -> None:
    order: list[str] = []
    progress_started = threading.Event()
    clear_started = threading.Event()

    class OrderedObserver(_CollectingObserver):
        def on_node_progress(self, event: NodeProgressEvent) -> None:
            if not event.persistent:
                return
            progress_started.set()
            assert clear_started.wait(1)
            super().on_node_progress(event)
            order.append("progress")

        def on_node_end(self, event: NodeExecutionEvent) -> None:
            super().on_node_end(event)
            order.append("node_end")

        def on_dag_end(self, event: DagRunEvent) -> None:
            super().on_dag_end(event)
            order.append("dag_end")

    original_clear = dag_runner._RunProgress.clear
    original_stop = dag_runner._RunProgress.stop

    def _record_clear(heartbeat, node) -> None:
        clear_started.set()
        original_clear(heartbeat, node)

    def _record_stop(heartbeat) -> None:
        original_stop(heartbeat)
        order.append("heartbeat_stop")

    monkeypatch.setattr(dag_runner._RunProgress, "clear", _record_clear)
    monkeypatch.setattr(dag_runner._RunProgress, "stop", _record_stop)
    observer = OrderedObserver()
    ctx = _context(tmp_path)
    ctx.heartbeat_interval_seconds = 0.01

    def _work():
        assert progress_started.wait(1)
        yield from ()

    dag = Dag(
        name="heartbeat-order-demo",
        nodes=(PipelineNode(name="produce", op=_work),),
    )

    assert list(run_dag(ctx, dag, observer=observer)) == []
    assert order == ["progress", "node_end", "heartbeat_stop", "dag_end"]


def test_progress_callback_can_clear_its_node() -> None:
    callback_finished = threading.Event()
    node = dag_runner.NodeProgressContext(
        dag_name="demo",
        node_name="work",
        node_index=0,
        execution_index=0,
        node_kind="function",
        node_calls_dag=None,
        depth=1,
    )

    class ReentrantObserver:
        def on_node_progress(self, event: NodeProgressEvent) -> None:
            progress.clear(node)
            callback_finished.set()

    progress = dag_runner._RunProgress(ReentrantObserver(), 0.001)
    progress.start_node(node)
    progress.enter(node)
    progress.start()
    try:
        assert callback_finished.wait(1)
    finally:
        progress.stop()


def test_run_dag_rejects_observer_without_node_progress_contract(
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
        dag_runner.report_node_progress(
            ProgressSnapshot(completed=0, phase="preparing")
        )
        yield 1

    dag = Dag(
        name="progress-demo",
        nodes=(PipelineNode(name="produce", op=_with_progress),),
    )

    with pytest.raises(AttributeError, match="on_node_progress"):
        list(run_dag(ctx, dag, observer=MinimalObserver()))


def test_run_dag_restores_context_when_dag_start_fails(tmp_path: Path) -> None:
    operation_called = False

    class FailingStartObserver(_CollectingObserver):
        def on_dag_start(self, **kwargs) -> None:
            raise RuntimeError("observer start failed")

    def _operation() -> list[int]:
        nonlocal operation_called
        operation_called = True
        return [1]

    observer = FailingStartObserver()
    dag = Dag(
        name="start-error",
        nodes=(PipelineNode(name="produce", op=_operation),),
    )

    with pytest.raises(RuntimeError, match="observer start failed"):
        list(run_dag(_context(tmp_path), dag, observer=observer))

    assert dag_runner._current_run_dag_depth() == 0
    assert dag_runner._CURRENT_ROOT_RUN.get() is None
    assert not operation_called
    assert observer.dag_events == []


def test_run_dag_propagates_error_and_marks_failure(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    def _explode(up):
        for value in up:
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
        for value in up:
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


def test_interleaved_root_runs_keep_interrupt_state_isolated(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    success_dag = Dag(
        name="success-demo",
        nodes=(PipelineNode(name="success-seed", op=lambda: [1, 2]),),
    )
    success_stream = run_dag(ctx, success_dag, observer=observer)
    assert next(success_stream) == 1

    interrupt_dag = Dag(
        name="interrupt-demo",
        nodes=(
            PipelineNode(name="seed", op=lambda: [1, 2]),
            PipelineNode(
                name="interrupt",
                op=lambda up: (
                    value if value == 1 else (_ for _ in ()).throw(KeyboardInterrupt())
                    for value in up
                ),
                input="seed",
            ),
        ),
    )

    with pytest.raises(KeyboardInterrupt):
        list(run_dag(ctx, interrupt_dag, observer=observer))

    assert list(success_stream) == [2]
    success_event = observer.dag_events[-1]
    assert (success_event.dag_name, success_event.status) == ("success-demo", "success")


def test_run_dag_restores_context_for_nested_siblings_across_yields(
    tmp_path: Path,
) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)
    ctx.heartbeat_interval_seconds = 999

    def _children():
        first_child = Dag(
            name="first-child",
            nodes=(PipelineNode(name="first-seed", op=lambda: [1, 2]),),
        )
        first = run_dag(ctx, first_child, observer=observer)
        try:
            yield next(first)
            assert dag_runner._CURRENT_RUN_PROGRESS.get() is not None
            assert dag_runner._CURRENT_ROOT_RUN.get() is not None
            second_child = Dag(
                name="second-child",
                nodes=(PipelineNode(name="second-seed", op=lambda: [3]),),
            )
            yield from run_dag(ctx, second_child, observer=observer)
            yield from first
        finally:
            first.close()

    outer = Dag(
        name="outer",
        nodes=(PipelineNode(name="open_children", op=_children),),
    )

    output_stream = run_dag(ctx, outer, observer=observer)
    output = []
    try:
        output.append(next(output_stream))
        assert (
            dag_runner._current_run_dag_depth(),
            dag_runner._CURRENT_RUN_PROGRESS.get(),
            dag_runner._CURRENT_ROOT_RUN.get(),
        ) == (0, None, None)
        unrelated = Dag(
            name="unrelated",
            nodes=(PipelineNode(name="unrelated-seed", op=lambda: [9]),),
        )
        assert list(run_dag(ctx, unrelated, observer=observer)) == [9]
        output.extend(output_stream)
    finally:
        output_stream.close()
    assert output == [1, 3, 2]
    assert (
        dag_runner._current_run_dag_depth(),
        dag_runner._CURRENT_RUN_PROGRESS.get(),
        dag_runner._CURRENT_ROOT_RUN.get(),
    ) == (0, None, None)

    expected_parent = DagParentRef("outer", "open_children", 0)
    assert {
        event.dag_name: (event.depth, event.parent) for event in observer.dag_events
    } == {
        "outer": (0, None),
        "first-child": (1, expected_parent),
        "second-child": (1, expected_parent),
        "unrelated": (0, None),
    }
    start_parent_by_name = {
        dag_name: parent
        for (dag_name, _), parent in zip(
            observer.dag_started, observer.dag_start_parents
        )
    }
    assert start_parent_by_name["outer"] is None
    assert start_parent_by_name["first-child"] == expected_parent
    assert start_parent_by_name["second-child"] == expected_parent
    assert start_parent_by_name["unrelated"] is None
    assert {
        event.node_name: (event.depth, event.execution_index)
        for event in observer.node_events
    } == {
        "open_children": (1, 0),
        "first-seed": (2, 1),
        "second-seed": (2, 2),
        "unrelated-seed": (1, 0),
    }


def test_preconstructed_seed_dag_joins_consuming_root_run(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    ingest = Dag(
        name="ingest",
        nodes=(PipelineNode(name="open_source", op=lambda: [1, 2]),),
    )
    records = run_dag(ctx, ingest, observer=observer)

    feature = Dag(
        name="feature",
        nodes=(
            PipelineNode(
                name="build_feature_stream",
                op=lambda seed: seed,
                input="seed",
            ),
        ),
    )
    features = run_dag(ctx, feature, seed=records, observer=observer)

    assert list(features) == [1, 2]
    assert observer.dag_started == [("feature", 1), ("ingest", 1)]
    assert observer.dag_start_depths == [0, 1]
    assert observer.dag_start_parents == [
        None,
        DagParentRef("feature", "build_feature_stream", 0),
    ]
    assert {
        event.node_name: event.depth for event in observer.node_events
    } == {
        "build_feature_stream": 1,
        "open_source": 2,
    }
    assert {event.execution_index for event in observer.node_events} == {0, 1}


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
                    op=lambda records: records,
                    input="seed",
                ),
            ),
        )
        return run_dag(ctx, feature, seed=_ingest_stream(), observer=observer)

    outer = Dag(
        name="vector",
        nodes=(PipelineNode(name="feature_stream", op=_feature_stream),),
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

    node_depth_by_name = {
        event.node_name: event.depth for event in observer.node_events
    }
    assert node_depth_by_name["feature_stream"] == 1
    assert node_depth_by_name["build_feature_stream"] == 2
    assert node_depth_by_name["open_source"] == 3


def test_run_dag_tracks_empty_nodes(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)

    dag = Dag(
        name="empty-demo",
        nodes=(
            PipelineNode(name="seed", op=lambda: []),
            PipelineNode(name="passthrough", op=lambda up: up, input="seed"),
        ),
    )

    output = list(run_dag(ctx, dag, observer=observer))
    assert output == []
    assert [event.node_name for event in observer.node_events] == [
        "seed",
        "passthrough",
    ]
    assert [event.output_items for event in observer.node_events] == [0, 0]
    assert observer.dag_events[-1].output_items == 0


def test_run_dag_rejects_none_node_output_during_setup(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    ctx = _context(tmp_path)
    called_nodes: list[str] = []

    def broken_node() -> None:
        called_nodes.append("broken")
        assert observer.dag_started == [("none-output", 2)]

    def valid_later_node() -> list[int]:
        called_nodes.append("later")
        return [1]

    dag = Dag(
        name="none-output",
        nodes=(
            PipelineNode(name="broken", op=broken_node),
            PipelineNode(name="later", op=valid_later_node),
        ),
    )

    stream = run_dag(ctx, dag, observer=observer)
    assert called_nodes == []
    assert observer.dag_started == []

    with pytest.raises(TypeError) as exc_info:
        list(stream)

    assert str(exc_info.value) == (
        "Node 'broken' in DAG 'none-output' returned None; "
        "node operations must return an iterable. "
        "Return () for an empty stream."
    )
    assert called_nodes == ["broken"]
    assert observer.dag_started == [("none-output", 2)]
    assert observer.node_started == []
    assert observer.node_events == []
    assert observer.dag_events[-1].status == "error"
    assert observer.dag_events[-1].error_type == "TypeError"


def test_run_dag_empty_dag_preserves_boundary_behavior(tmp_path: Path) -> None:
    class BrokenSeed:
        def __iter__(self):
            raise RuntimeError("seed iteration failed")

    ctx = _context(tmp_path)
    dag = Dag(name="empty", nodes=())

    empty_observer = _CollectingObserver()
    assert list(run_dag(ctx, dag, observer=empty_observer)) == []
    assert empty_observer.node_events == []
    assert empty_observer.dag_events[-1].status == "success"
    assert empty_observer.dag_events[-1].output_items == 0

    seeded_observer = _CollectingObserver()
    assert list(run_dag(ctx, dag, seed=[1, 2], observer=seeded_observer)) == [1, 2]
    assert seeded_observer.node_events == []
    assert seeded_observer.dag_events[-1].status == "success"
    assert seeded_observer.dag_events[-1].output_items == 2

    broken_observer = _CollectingObserver()
    with pytest.raises(RuntimeError, match="seed iteration failed"):
        list(run_dag(ctx, dag, seed=BrokenSeed(), observer=broken_observer))
    assert broken_observer.dag_events[-1].status == "error"
    assert broken_observer.dag_events[-1].error_type == "RuntimeError"
    assert dag_runner._current_run_dag_depth() == 0


def test_run_dag_executes_validated_kwinput_bindings(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    combine_inputs = {"right": "right"}

    def _left():
        combine_inputs["right"] = "left"
        return [1, 3]

    dag = Dag(
        name="fan-in-demo",
        nodes=(
            PipelineNode(name="left", op=_left),
            PipelineNode(name="right", op=lambda: [2, 4]),
            PipelineNode(
                name="combine",
                op=lambda left, *, right: zip(left, right),
                input="left",
                kwinputs=combine_inputs,
            ),
        ),
    )

    assert list(run_dag(ctx, dag)) == [(1, 2), (3, 4)]


def test_run_dag_rejects_fan_out_before_operations(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    source_called = False

    def _source():
        nonlocal source_called
        source_called = True
        return iter([1, 2, 3, 4])

    dag = Dag(
        name="fan-out-demo",
        nodes=(
            PipelineNode(name="source", op=_source, output="records"),
            PipelineNode(name="left", op=lambda up: up, input="records"),
            PipelineNode(name="right", op=lambda up: up, input="records"),
        ),
    )

    with pytest.raises(ValueError, match="node 'left'.*node 'right'"):
        list(run_dag(ctx, dag))

    assert not source_called


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
