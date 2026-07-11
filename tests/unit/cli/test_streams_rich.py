from io import StringIO
import logging

from rich.console import Console
from rich.progress import Progress

from datapipeline.cli.visuals.execution import (
    DagFinished,
    DagStarted,
    ExecutionMessage,
    NodeFinished,
    NodeProgress,
    NodeStarted,
    SourceInfoMessage,
)
from datapipeline.cli.visuals.execution_context import (
    current_execution_event_sink,
    current_terminal_log_proxy_sink,
    reset_current_execution_event_sink,
    reset_current_terminal_log_proxy_sink,
    set_current_execution_event_sink,
    set_current_terminal_log_proxy_sink,
)
from datapipeline.cli.visuals.rich.event_sink import _RichConsoleExecutionSink
from datapipeline.cli.visuals.rich.progress import (
    _ExecutionProgress,
    visual_execution,
)
from datapipeline.dag.events import ProgressResource, ProgressSnapshot


def _console() -> tuple[Console, StringIO]:
    output = StringIO()
    return (
        Console(file=output, markup=False, highlight=False, force_terminal=False),
        output,
    )


def _progress() -> Progress:
    console, _ = _console()
    return Progress(console=console, auto_refresh=False)


def _start_node(
    renderer: _ExecutionProgress,
    execution_index: int,
    node_name: str,
) -> None:
    renderer.handle(
        NodeStarted(
            dag_name="stream:adv.20",
            node_name=node_name,
            node_index=execution_index,
            execution_index=execution_index,
            depth=1,
        )
    )


def _finish_node(
    renderer: _ExecutionProgress,
    execution_index: int,
    node_name: str,
) -> None:
    renderer.handle(
        NodeFinished(
            dag_name="stream:adv.20",
            node_name=node_name,
            node_index=execution_index,
            execution_index=execution_index,
            status="success",
            output_items=100,
            elapsed_seconds=1,
            depth=1,
        )
    )


def _node_progress(
    execution_index: int,
    node_name: str,
    snapshot: ProgressSnapshot,
) -> NodeProgress:
    return NodeProgress(
        dag_name="stream:adv.20",
        node_name=node_name,
        node_index=execution_index,
        execution_index=execution_index,
        progress=snapshot,
        elapsed_seconds=1,
        depth=1,
    )


def test_info_progress_uses_one_root_dag_row_and_deepest_active_node() -> None:
    progress = _progress()
    renderer = _ExecutionProgress(progress, debug=False)

    renderer.handle(DagStarted(dag_name="stream:adv.20", node_count=4))
    _start_node(renderer, 0, "order_records")
    renderer.handle(
        _node_progress(
            0,
            "order_records",
            ProgressSnapshot(completed=100, phase="reading", unit="records"),
        )
    )
    _start_node(renderer, 1, "open_source")
    renderer.handle(
        _node_progress(
            1,
            "open_source",
            ProgressSnapshot(
                completed=20_000,
                unit="records",
                phase="streaming from",
                resource=ProgressResource(2, 17, '"2011.jsonl"'),
            ),
        )
    )

    assert len(progress.tasks) == 1
    task = progress.tasks[0]
    assert task.description == "stream:adv.20"
    assert task.total == 17
    assert task.completed == 1
    assert task.fields["status"] == (
        'open_source · streaming from · 2/17 "2011.jsonl" '
        '· 20,000 records'
    )

    _finish_node(renderer, 1, "open_source")

    task = progress.tasks[0]
    assert task.total is None
    assert task.fields["status"] == (
        "order_records · reading · 100 records"
    )


def test_info_progress_ignores_updates_from_nodes_behind_active_node() -> None:
    progress = _progress()
    renderer = _ExecutionProgress(progress, debug=False)
    renderer.handle(DagStarted(dag_name="stream:adv.20", node_count=4))
    _start_node(renderer, 0, "order_records")
    _start_node(renderer, 1, "open_source")

    renderer.handle(
        _node_progress(
            1,
            "open_source",
            ProgressSnapshot(completed=10, phase="reading"),
        )
    )
    visible_status = progress.tasks[0].fields["status"]
    renderer.handle(
        _node_progress(
            0,
            "order_records",
            ProgressSnapshot(completed=10, phase="sorting"),
        )
    )

    assert progress.tasks[0].fields["status"] == visible_status


def test_debug_progress_has_one_row_per_active_node() -> None:
    progress = _progress()
    renderer = _ExecutionProgress(progress, debug=True)
    renderer.handle(DagStarted(dag_name="stream:adv.20", node_count=4))
    _start_node(renderer, 0, "order_records")
    _start_node(renderer, 1, "open_source")

    renderer.handle(
        _node_progress(
            1,
            "open_source",
            ProgressSnapshot(completed=25, total=100, unit="records"),
        )
    )

    assert [task.description for task in progress.tasks] == [
        "stream:adv.20/order_records",
        "stream:adv.20/open_source",
    ]
    source_task = progress.tasks[1]
    assert source_task.completed == 25
    assert source_task.total == 100
    assert source_task.fields["status"] == "25/100 records"

    _finish_node(renderer, 1, "open_source")
    assert [task.description for task in progress.tasks] == [
        "stream:adv.20/order_records"
    ]


def test_root_dag_finish_clears_progress_and_remains_a_static_event() -> None:
    progress = _progress()
    renderer = _ExecutionProgress(progress, debug=False)
    renderer.handle(DagStarted(dag_name="stream:adv.20", node_count=4))

    renderer.handle(
        DagFinished(
            dag_name="stream:adv.20",
            node_count=4,
            status="success",
            output_items=100,
            elapsed_seconds=1,
        )
    )

    assert progress.tasks == []


def test_rich_sink_routes_node_progress_to_renderer_without_printing() -> None:
    console, output = _console()

    class _Renderer:
        def __init__(self) -> None:
            self.events = []

        def handle(self, event) -> None:
            self.events.append(event)

    renderer = _Renderer()
    sink = _RichConsoleExecutionSink(logging.INFO, console, renderer)
    event = _node_progress(0, "open_source", ProgressSnapshot(completed=10))

    sink.emit(event)

    assert renderer.events == [event]
    assert output.getvalue() == ""


def test_rich_sink_keeps_static_messages_and_source_information() -> None:
    console, output = _console()
    sink = _RichConsoleExecutionSink(logging.INFO, console)

    sink.emit(ExecutionMessage(message="Saved 10 records"))
    sink.emit(
        SourceInfoMessage(
            depth=1,
            source_label="ingest:ohlcv",
            message="fs.glob count=17",
        )
    )

    assert output.getvalue().splitlines() == [
        "Saved 10 records",
        "  [ingest:ohlcv] fs.glob count=17",
    ]


def test_visual_execution_restores_existing_context(monkeypatch) -> None:
    console, _ = _console()
    monkeypatch.setattr(
        "datapipeline.cli.visuals.rich.progress.Console",
        lambda **kwargs: console,
    )
    event_token = set_current_execution_event_sink("previous-event-sink")
    proxy_token = set_current_terminal_log_proxy_sink("previous-proxy-sink")
    try:
        with visual_execution(logging.INFO):
            assert current_execution_event_sink() != "previous-event-sink"
            assert current_terminal_log_proxy_sink() != "previous-proxy-sink"
        assert current_execution_event_sink() == "previous-event-sink"
        assert current_terminal_log_proxy_sink() == "previous-proxy-sink"
    finally:
        reset_current_terminal_log_proxy_sink(proxy_token)
        reset_current_execution_event_sink(event_token)
