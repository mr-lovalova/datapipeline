from io import StringIO
import logging
from pathlib import Path

from rich.console import Console
from rich.progress import BarColumn, Progress

from datapipeline.cli.visuals.execution import (
    PipelineFinished,
    PipelineStarted,
    ExecutionMessage,
    NodeFinished,
    NodeProgress,
    NodeStarted,
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
from datapipeline.execution.events import ProgressResource, ProgressSnapshot
from datapipeline.execution.observability import (
    FileResult,
    OperationFinished,
    OperationStarted,
)


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
    node_index: int,
    node_name: str,
) -> None:
    renderer.handle(
        NodeStarted(
            pipeline_name="stream:adv.20",
            node_name=node_name,
            node_index=node_index,
        )
    )


def _finish_node(
    renderer: _ExecutionProgress,
    node_index: int,
    node_name: str,
) -> None:
    renderer.handle(
        NodeFinished(
            pipeline_name="stream:adv.20",
            node_name=node_name,
            node_index=node_index,
            status="success",
            output_items=100,
            elapsed_seconds=1,
        )
    )


def _node_progress(
    node_index: int,
    node_name: str,
    snapshot: ProgressSnapshot,
    persistent: bool = False,
) -> NodeProgress:
    return NodeProgress(
        pipeline_name="stream:adv.20",
        node_name=node_name,
        node_index=node_index,
        progress=snapshot,
        elapsed_seconds=1,
        persistent=persistent,
    )


def _info_progress(*node_names: str) -> tuple[Progress, _ExecutionProgress]:
    progress = _progress()
    renderer = _ExecutionProgress(progress, debug=False)
    renderer.handle(PipelineStarted(pipeline_name="stream:adv.20", node_count=4))
    for node_index, node_name in enumerate(node_names):
        _start_node(renderer, node_index, node_name)
    return progress, renderer


def test_info_progress_shows_node_and_local_detail() -> None:
    progress, renderer = _info_progress("order_records")
    _start_node(
        renderer,
        1,
        "open_source",
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
    assert task.fields["show_elapsed"] is True
    assert task.total is None
    assert task.completed == 20_000
    assert task.fields["status"] == (
        'stream:adv.20/open_source · 2/17 "2011.jsonl" · 20,000 records'
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


def test_info_pipeline_elapsed_continues_after_completed_local_phase() -> None:
    now = 0.0
    console, _ = _console()
    progress = Progress(
        console=console,
        auto_refresh=False,
        get_time=lambda: now,
    )
    renderer = _ExecutionProgress(progress, debug=False)
    renderer.handle(PipelineStarted(pipeline_name="stream:adv.20", node_count=4))
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
    elapsed = _LiveElapsedColumn().render(task)
    assert elapsed.plain == "PIPELINE 0:00:10"
    assert str(elapsed.style) == "dim"
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
    assert _LiveElapsedColumn().render(task).plain == "PIPELINE 0:00:20"
    assert task.description == "stream:adv.20"
    assert task.fields["indent"] == ""


def test_debug_progress_shows_root_and_active_nodes() -> None:
    progress = _progress()
    renderer = _ExecutionProgress(progress, debug=True)
    renderer.handle(PipelineStarted(pipeline_name="stream:adv.20", node_count=4))
    _start_node(renderer, 0, "order_records")
    _start_node(renderer, 1, "open_source")
    _start_node(
        renderer,
        2,
        "decode_records",
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
            task.fields["show_elapsed"],
            task.fields["status"],
        )
        for task in tasks
    ] == [
        ("stream:adv.20", "", True, ""),
        ("stream:adv.20/order_records", "", False, "0 out"),
        ("stream:adv.20/open_source", "", False, "25/100 records"),
        ("stream:adv.20/decode_records", "", False, "0 out"),
    ]
    root_task, _, source_task, _ = tasks
    elapsed = _LiveElapsedColumn()
    assert elapsed.render(root_task).plain == "PIPELINE 0:00:00"
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
        "stream:adv.20/decode_records",
    ]


def test_root_pipeline_finish_clears_progress_and_remains_a_static_event() -> None:
    progress = _progress()
    renderer = _ExecutionProgress(progress, debug=False)
    renderer.handle(PipelineStarted(pipeline_name="stream:adv.20", node_count=4))

    renderer.handle(
        PipelineFinished(
            pipeline_name="stream:adv.20",
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
        pipeline_name="stream:adv.20",
        node_name="rolling",
        node_index=3,
        status="success",
        output_items=100,
        elapsed_seconds=1,
    )

    sink.emit(event)

    assert renderer.events == [event]
    assert output.getvalue().strip() == (
        "[stream:adv.20/rolling] finished status=success out=100 elapsed=1.000000s"
    )


def test_rich_sink_hides_successful_node_finish_at_info() -> None:
    console, output = _console()
    renderer = _CaptureRenderer()
    sink = _RichConsoleExecutionSink(logging.INFO, console, renderer)
    event = NodeFinished(
        pipeline_name="stream:adv.20",
        node_name="rolling",
        node_index=3,
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
        pipeline_name="stream:adv.20",
        node_name="rolling",
        node_index=3,
        status="error",
        error_type="ValueError",
        error_message="invalid record",
        output_items=0,
        elapsed_seconds=1,
    )

    sink.emit(event)

    assert renderer.events == [event]
    assert output.getvalue().strip() == (
        "[stream:adv.20/rolling] finished "
        "status=error error=ValueError: invalid record out=0 elapsed=1.000000s"
    )


def test_rich_sink_persists_root_pipeline_lifecycle() -> None:
    console, output = _console()
    renderer = _CaptureRenderer()
    sink = _RichConsoleExecutionSink(logging.INFO, console, renderer)
    root = PipelineStarted(pipeline_name="stream:adv.20", node_count=4)
    root_finished = PipelineFinished(
        pipeline_name="stream:adv.20",
        node_count=4,
        status="success",
        output_items=100,
        elapsed_seconds=1,
    )
    events = [root, root_finished]

    for event in events:
        sink.emit(event)

    assert renderer.events == events
    assert output.getvalue().splitlines() == [
        "[stream:adv.20] started nodes=4",
        "[stream:adv.20] finished status=success items=100 elapsed=1.000000s",
    ]


def test_rich_sink_static_events_are_flat() -> None:
    console, output = _console()
    sink = _RichConsoleExecutionSink(logging.INFO, console)

    sink.emit(ExecutionMessage(message="Saved 10 records"))

    assert output.getvalue().splitlines() == ["Saved 10 records"]


def test_rich_sink_hides_debug_config_at_info() -> None:
    console, output = _console()
    sink = _RichConsoleExecutionSink(logging.INFO, console)

    sink.emit(
        ExecutionMessage(
            message="Config:\n{}",
            log_level=logging.DEBUG,
        )
    )

    assert output.getvalue() == ""


def test_rich_sink_renders_operation_sequence_once_and_in_order() -> None:
    console, output = _console()
    sink = _RichConsoleExecutionSink(logging.DEBUG, console)

    events = (
        OperationStarted("materialize:adv.20"),
        ExecutionMessage(
            message='Config:\n{"stream": "adv.20"}', log_level=logging.DEBUG
        ),
        PipelineStarted(pipeline_name="stream:adv.20", node_count=1),
        PipelineFinished(
            pipeline_name="stream:adv.20",
            node_count=1,
            status="success",
            output_items=10,
            elapsed_seconds=1,
        ),
        FileResult("Output", Path("/tmp/adv.20.jsonl")),
        OperationFinished("materialize:adv.20", "success", 1),
    )
    for event in events:
        sink.emit(event)

    rendered = output.getvalue()
    markers = [
        "Operation materialize:adv.20",
        "Config:",
        "[stream:adv.20] started",
        "[stream:adv.20] finished",
        "Output:",
        "Operation materialize:adv.20 finished",
    ]
    positions = [rendered.index(marker) for marker in markers]
    assert positions == sorted(positions)
    assert rendered.count("Config:") == 1
    assert "Operation materialize:adv.20 started" not in rendered


def test_rich_sink_renders_minimal_operation_header_at_info(monkeypatch) -> None:
    console, _ = _console()
    sink = _RichConsoleExecutionSink(logging.INFO, console)
    renderables = []
    monkeypatch.setattr(console, "print", renderables.append)

    sink.emit(OperationStarted("materialize:adv.20"))

    assert len(renderables) == 1
    segments = list(console.render(renderables[0], console.options))
    title = next(segment for segment in segments if "Operation" in segment.text)
    rules = [segment for segment in segments if "─" in segment.text]
    assert title.text == "Operation materialize:adv.20"
    assert str(title.style) == "none"
    assert rules and all(
        segment.style is not None and segment.style.dim for segment in rules
    )


def test_rich_sink_hides_operation_header_at_warning() -> None:
    console, output = _console()
    sink = _RichConsoleExecutionSink(logging.WARNING, console)

    sink.emit(OperationStarted("materialize:adv.20"))

    assert output.getvalue() == ""


def test_rich_sink_styles_only_final_status() -> None:
    console, _ = _console()
    sink = _RichConsoleExecutionSink(logging.INFO, console)
    success = sink._render_event(
        PipelineFinished(
            pipeline_name="stream:adv.20",
            node_count=4,
            status="success",
            output_items=100,
            elapsed_seconds=1,
        )
    )
    operation_error = sink._render_event(
        OperationFinished(
            name="materialize:adv.20",
            status="error",
            elapsed_seconds=1,
            error_type="ValueError",
        )
    )

    assert success.style == operation_error.style == ""
    assert [
        (success.plain[span.start : span.end], str(span.style))
        for span in success.spans
    ] == [("status=success", "green")]
    assert [
        (operation_error.plain[span.start : span.end], str(span.style))
        for span in operation_error.spans
    ] == [("status=error", "red")]


def test_rich_sink_renders_debug_config_as_regular_text() -> None:
    console, _ = _console()
    sink = _RichConsoleExecutionSink(logging.DEBUG, console)

    text = sink._render_event(
        ExecutionMessage(message="Config:\n{}", log_level=logging.DEBUG)
    )

    assert text.style == ""


def test_rich_sink_renders_file_result_as_aligned_link() -> None:
    console, output = _console(width=60)
    path = Path(
        "/Users/anders/project/artifacts/smoke/processed/very-long/"
        "dataset/dataset.train_0.jsonl"
    )
    event = FileResult(label="train_0", path=path)
    sink = _RichConsoleExecutionSink(logging.INFO, console)

    sink.emit(event)

    lines = output.getvalue().splitlines()
    assert lines[0].startswith("train_0: ")
    assert lines[1].startswith(" " * 9)

    table = sink._render_file_result(event)
    segments = list(console.render(table, console.options))
    label = next(segment for segment in segments if segment.text == "train_0:")
    assert label.style is None or not label.style.bold
    linked = [
        segment
        for segment in segments
        if segment.style is not None and segment.style.link == path.resolve().as_uri()
    ]
    assert "".join(segment.text for segment in linked) == str(path)
    assert all(
        segment.style.color is not None and segment.style.color.name == "blue"
        for segment in linked
    )


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


def test_visual_execution_uses_minimal_progress_styles(monkeypatch) -> None:
    console, _ = _console()
    columns = []

    def capture_progress(*args, **kwargs):
        columns.extend(args)
        return Progress(*args, **kwargs)

    monkeypatch.setattr(
        "datapipeline.cli.visuals.rich.progress.Console",
        lambda **_kwargs: console,
    )
    monkeypatch.setattr(
        "datapipeline.cli.visuals.rich.progress.Progress",
        capture_progress,
    )

    with visual_execution(logging.INFO):
        pass

    label, bar = columns[:2]
    assert label.style == "none"
    assert bar.complete_style == "cyan"
    assert bar.finished_style == "cyan"
