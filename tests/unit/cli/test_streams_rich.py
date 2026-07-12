from io import StringIO
import logging
from pathlib import Path

from rich.console import Console
from rich.progress import BarColumn, Progress

from datapipeline.cli.visuals.execution import (
    DagFinished,
    DagStarted,
    ExecutionMessage,
    NodeFinished,
    NodeProgress,
    NodeStarted,
    ProfileStarted,
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
    _LiveElapsedColumn,
    visual_execution,
)
from datapipeline.dag.events import DagParentRef, ProgressResource, ProgressSnapshot
from datapipeline.execution.observability import FileResult


def _console(width: int | None = None) -> tuple[Console, StringIO]:
    output = StringIO()
    return (
        Console(
            file=output,
            markup=False,
            highlight=False,
            force_terminal=False,
            width=width,
        ),
        output,
    )


def _progress() -> Progress:
    console, _ = _console()
    return Progress(console=console, auto_refresh=False)


class _CaptureRenderer:
    def __init__(self) -> None:
        self.events = []

    def handle(self, event) -> None:
        self.events.append(event)


def _start_node(
    renderer: _ExecutionProgress,
    execution_index: int,
    node_name: str,
    dag_name: str = "stream:adv.20",
    depth: int = 1,
) -> None:
    renderer.handle(
        NodeStarted(
            dag_name=dag_name,
            node_name=node_name,
            node_index=execution_index,
            execution_index=execution_index,
            depth=depth,
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
    persistent: bool = False,
) -> NodeProgress:
    return NodeProgress(
        dag_name="stream:adv.20",
        node_name=node_name,
        node_index=execution_index,
        execution_index=execution_index,
        progress=snapshot,
        elapsed_seconds=1,
        persistent=persistent,
        depth=1,
    )


def _info_progress(*node_names: str) -> tuple[Progress, _ExecutionProgress]:
    progress = _progress()
    renderer = _ExecutionProgress(progress, debug=False)
    renderer.handle(DagStarted(dag_name="stream:adv.20", node_count=4))
    for execution_index, node_name in enumerate(node_names):
        _start_node(renderer, execution_index, node_name)
    return progress, renderer


def test_info_progress_shows_node_and_local_detail() -> None:
    progress, renderer = _info_progress("order_records")
    _start_node(
        renderer,
        1,
        "open_source",
        dag_name="ingest:ohlcv",
        depth=2,
    )
    renderer.handle(
        _node_progress(
            0,
            "order_records",
            ProgressSnapshot(completed=100, phase="reading", unit="records"),
        )
    )
    renderer.handle(
        _node_progress(
            1,
            "open_source",
            ProgressSnapshot(
                completed=20_000,
                unit="records",
                resource=ProgressResource(2, 17, '"2011.jsonl"'),
            ),
        )
    )

    assert len(progress.tasks) == 1
    task = progress.tasks[0]
    assert task.description == "stream:adv.20"
    assert task.fields["indent"] == ""
    assert task.fields["timer_scope"] == "DAG"
    assert task.total is None
    assert task.completed == 20_000
    assert task.fields["status"] == (
        'ingest:ohlcv/open_source · 2/17 "2011.jsonl" · 20,000 records'
    )

    _finish_node(renderer, 1, "open_source")

    task = progress.tasks[0]
    assert task.description == "stream:adv.20"
    assert task.fields["indent"] == ""
    assert task.total is None
    assert task.fields["status"] == (
        "stream:adv.20/order_records · reading · 100 records"
    )


def test_info_progress_selects_meaningful_event_owner() -> None:
    progress, renderer = _info_progress(
        "order_records",
        "map_records",
        "open_source",
    )
    renderer.handle(
        _node_progress(
            2,
            "open_source",
            ProgressSnapshot(
                completed=20_000,
                resource=ProgressResource(2, 17, '"2011.jsonl"'),
            ),
        )
    )
    task = progress.tasks[0]
    assert task.description == "stream:adv.20"
    assert task.fields["status"] == (
        'stream:adv.20/open_source · 2/17 "2011.jsonl" · 20,000 items'
    )

    renderer.handle(
        _node_progress(1, "map_records", ProgressSnapshot(completed=20_000))
    )
    task = progress.tasks[0]
    assert task.description == "stream:adv.20"
    assert task.fields["status"] == (
        'stream:adv.20/open_source · 2/17 "2011.jsonl" · 20,000 items'
    )

    renderer.handle(
        _node_progress(
            0,
            "order_records",
            ProgressSnapshot(completed=25, total=100, phase="merging"),
        )
    )
    task = progress.tasks[0]
    assert task.description == "stream:adv.20"
    assert task.completed == 25
    assert task.total == 100
    assert task.fields["status"] == (
        "stream:adv.20/order_records · merging · 25/100 items"
    )

    renderer.handle(
        _node_progress(
            1,
            "map_records",
            ProgressSnapshot(completed=20_000),
            persistent=True,
        )
    )

    task = progress.tasks[0]
    assert task.description == "stream:adv.20"
    assert task.fields["status"] == "stream:adv.20/map_records · 20,000 items"


def test_info_dag_elapsed_continues_after_completed_local_phase() -> None:
    now = 0.0
    console, _ = _console()
    progress = Progress(
        console=console,
        auto_refresh=False,
        get_time=lambda: now,
    )
    renderer = _ExecutionProgress(progress, debug=False)
    renderer.handle(DagStarted(dag_name="stream:adv.20", node_count=4))
    _start_node(renderer, 0, "order_records")

    now = 10.0
    renderer.handle(
        _node_progress(
            0,
            "order_records",
            ProgressSnapshot(completed=100, total=100, phase="merging"),
        )
    )
    task = progress.tasks[0]
    assert _LiveElapsedColumn().render(task).plain == "0:00:10"
    assert task.description == "stream:adv.20"
    assert task.fields["indent"] == ""

    now = 20.0
    renderer.handle(
        _node_progress(
            0,
            "order_records",
            ProgressSnapshot(completed=20, total=100, phase="emitting"),
        )
    )

    task = progress.tasks[0]
    assert _LiveElapsedColumn().render(task).plain == "0:00:20"
    assert task.description == "stream:adv.20"
    assert task.fields["indent"] == ""


def test_debug_progress_shows_root_and_active_nodes() -> None:
    progress = _progress()
    renderer = _ExecutionProgress(progress, debug=True)
    renderer.handle(DagStarted(dag_name="stream:adv.20", node_count=4))
    _start_node(renderer, 0, "order_records")
    _start_node(renderer, 1, "open_source")
    _start_node(
        renderer,
        2,
        "decode_records",
        dag_name="ingest:ohlcv",
        depth=2,
    )

    renderer.handle(
        _node_progress(
            1,
            "open_source",
            ProgressSnapshot(completed=25, total=100, unit="records"),
        )
    )

    tasks = progress.tasks
    assert [
        (
            task.description,
            task.fields["indent"],
            task.fields["timer_scope"],
            task.fields["status"],
        )
        for task in tasks
    ] == [
        ("stream:adv.20", "", "DAG", ""),
        ("stream:adv.20/order_records", "", "", "0 out"),
        ("stream:adv.20/open_source", "", "", "25/100 records"),
        ("ingest:ohlcv/decode_records", "  ", "", "0 out"),
    ]
    root_task, _, source_task, _ = tasks
    elapsed = _LiveElapsedColumn()
    assert elapsed.render(root_task).plain == "0:00:00"
    assert elapsed.render(source_task).plain == ""
    assert source_task.completed == 25
    assert source_task.total == 100
    bar = BarColumn().render(source_task)
    assert bar.completed == 25
    assert bar.total == 100

    _finish_node(renderer, 1, "open_source")
    assert [task.description for task in progress.tasks] == [
        "stream:adv.20",
        "stream:adv.20/order_records",
        "ingest:ohlcv/decode_records",
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
    renderer = _CaptureRenderer()
    sink = _RichConsoleExecutionSink(logging.INFO, console, renderer)
    event = _node_progress(0, "open_source", ProgressSnapshot(completed=10))

    sink.emit(event)

    assert renderer.events == [event]
    assert output.getvalue() == ""


def test_rich_sink_persists_node_finish_at_debug() -> None:
    console, output = _console(width=120)
    renderer = _CaptureRenderer()
    sink = _RichConsoleExecutionSink(logging.DEBUG, console, renderer)
    event = NodeFinished(
        dag_name="stream:adv.20",
        node_name="stream_transforms",
        node_index=3,
        execution_index=0,
        status="success",
        output_items=100,
        elapsed_seconds=1,
    )

    sink.emit(event)

    assert renderer.events == [event]
    assert output.getvalue().strip() == (
        "[stream:adv.20/stream_transforms] finished "
        "status=success out=100 elapsed=1.000000s"
    )


def test_rich_sink_hides_successful_node_finish_at_info() -> None:
    console, output = _console()
    renderer = _CaptureRenderer()
    sink = _RichConsoleExecutionSink(logging.INFO, console, renderer)
    event = NodeFinished(
        dag_name="stream:adv.20",
        node_name="stream_transforms",
        node_index=3,
        execution_index=0,
        status="success",
        output_items=100,
        elapsed_seconds=1,
    )

    sink.emit(event)

    assert renderer.events == [event]
    assert output.getvalue() == ""


def test_rich_sink_persists_failed_node_finish_at_info() -> None:
    console, output = _console(width=140)
    renderer = _CaptureRenderer()
    sink = _RichConsoleExecutionSink(logging.INFO, console, renderer)
    event = NodeFinished(
        dag_name="stream:adv.20",
        node_name="stream_transforms",
        node_index=3,
        execution_index=0,
        status="error",
        error_type="ValueError",
        error_message="invalid record",
        output_items=0,
        elapsed_seconds=1,
    )

    sink.emit(event)

    assert renderer.events == [event]
    assert output.getvalue().strip() == (
        "[stream:adv.20/stream_transforms] finished "
        "status=error error=ValueError: invalid record out=0 elapsed=1.000000s"
    )


def test_rich_sink_persists_all_dag_lifecycle() -> None:
    console, output = _console()
    renderer = _CaptureRenderer()
    sink = _RichConsoleExecutionSink(logging.INFO, console, renderer)
    root = DagStarted(dag_name="stream:adv.20", node_count=4)
    parent = DagParentRef(
        dag_name="stream:adv.20",
        node_name="open_source",
        node_index=2,
    )
    nested = DagStarted(
        dag_name="ingest:ohlcv",
        node_count=3,
        depth=1,
        dag_parent=parent,
    )
    nested_finished = DagFinished(
        dag_name="ingest:ohlcv",
        node_count=3,
        status="success",
        output_items=100,
        elapsed_seconds=1,
        depth=1,
        dag_parent=parent,
    )
    root_finished = DagFinished(
        dag_name="stream:adv.20",
        node_count=4,
        status="success",
        output_items=100,
        elapsed_seconds=1,
    )
    events = [root, nested, nested_finished, root_finished]

    for event in events:
        sink.emit(event)

    assert renderer.events == events
    assert output.getvalue().splitlines() == [
        "[stream:adv.20] started nodes=4",
        "  [ingest:ohlcv] started nodes=3",
        "  [ingest:ohlcv] finished status=success items=100 elapsed=1.000000s",
        "[stream:adv.20] finished status=success items=100 elapsed=1.000000s",
    ]


def test_rich_sink_static_events_use_only_event_depth() -> None:
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


def test_rich_sink_renders_profile_header_once_at_warning() -> None:
    console, output = _console()
    sink = _RichConsoleExecutionSink(logging.WARNING, console)

    sink.emit(
        ProfileStarted(
            command="materialize",
            name="adv.20",
            index=1,
            total=14,
        )
    )

    lines = output.getvalue().splitlines()
    assert sum("Materialize Profiles" in line for line in lines) == 1
    assert sum("── adv.20 (1/14) ──" in line for line in lines) == 1
    assert [line for line in lines if line.strip()][1] == "  ── adv.20 (1/14) ──"


def test_rich_sink_styles_labels_and_final_status() -> None:
    console, _ = _console()
    sink = _RichConsoleExecutionSink(logging.INFO, console)
    success = sink._render_event(
        DagFinished(
            dag_name="stream:adv.20",
            node_count=4,
            status="success",
            output_items=100,
            elapsed_seconds=1,
        )
    )
    error = sink._render_event(
        DagFinished(
            dag_name="stream:adv.20",
            node_count=4,
            status="error",
            output_items=0,
            elapsed_seconds=1,
        )
    )

    assert any(str(span.style) == "bold cyan" for span in success.spans)
    assert any(str(span.style) == "green" for span in success.spans)
    assert any(str(span.style) == "bold red" for span in error.spans)


def test_rich_sink_renders_file_result_as_aligned_link() -> None:
    console, output = _console(width=60)
    path = Path(
        "/Users/anders/project/artifacts/smoke/processed/very-long/"
        "dataset/dataset.train_0.jsonl"
    )
    event = FileResult(label="train_0", path=path, records=100)
    sink = _RichConsoleExecutionSink(logging.INFO, console)

    sink.emit(event)

    lines = output.getvalue().splitlines()
    assert lines[0].startswith("train_0: ")
    assert lines[1].startswith(" " * 9)

    table = sink._render_file_result(event)
    segments = list(console.render(table, console.options))
    assert any(
        segment.text == "train_0:" and segment.style is not None and segment.style.bold
        for segment in segments
    )
    assert any(
        segment.style is not None
        and segment.style.link == path.resolve().as_uri()
        and segment.style.color is not None
        and segment.style.color.name == "bright_blue"
        for segment in segments
    )
    assert any(
        "100 records" in segment.text
        and segment.style is not None
        and segment.style.dim
        and segment.style.link is None
        for segment in segments
    )


def test_rich_sink_filters_info_outputs_below_its_log_level() -> None:
    console, output = _console()
    sink = _RichConsoleExecutionSink(logging.WARNING, console)
    sink.emit(FileResult(label="Output", path=Path("data/adv.20.jsonl")))
    sink.emit(ExecutionMessage(message="Metadata: data/adv.20.metadata.json"))

    assert output.getvalue() == ""


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
