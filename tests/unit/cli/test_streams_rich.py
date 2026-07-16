from io import StringIO
import logging
from pathlib import Path
import subprocess
import sys
from textwrap import dedent
from types import SimpleNamespace
from unittest.mock import patch

from rich.console import Console
from rich.progress import Progress
from rich.table import Column

import datapipeline.cli.visuals.rich.progress as rich_progress
from datapipeline.cli.visuals.execution import (
    ExecutionMessage,
)
from datapipeline.cli.visuals.execution_context import (
    current_execution_event_handler,
    current_terminal_log_handler,
    reset_current_execution_event_handler,
    reset_current_terminal_log_handler,
    set_current_execution_event_handler,
    set_current_terminal_log_handler,
)
from datapipeline.cli.visuals.rich.progress import (
    _ExecutionProgress,
    _NodeBarColumn,
    _ProgressLabelColumn,
    _RichExecutionRenderer,
    rich_visuals_supported,
    visual_execution,
)
from datapipeline.execution.events import (
    NodeFinished,
    NodeProgress,
    NodeStarted,
    PipelineFinished,
    PipelineProgress,
    PipelineStarted,
    ProgressResource,
    ProgressSnapshot,
)
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
    heartbeat: bool = False,
) -> NodeProgress:
    return NodeProgress(
        pipeline_name="stream:adv.20",
        node_name=node_name,
        node_index=node_index,
        progress=snapshot,
        elapsed_seconds=1,
        heartbeat=heartbeat,
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

    root, order_records, open_source = progress.tasks
    assert root.description == "stream:adv.20"
    assert root.fields["is_pipeline"] is True
    assert root.total is None
    assert root.completed == 0
    assert root.fields["status"] == ""
    assert root.visible is True
    assert order_records.visible is False
    assert open_source.visible is True
    assert open_source.completed == 20_000
    assert open_source.fields["status"] == (
        '2/17 "2011.jsonl" · 20,000 records'
    )

    with patch.object(progress, "refresh", wraps=progress.refresh) as refresh:
        _finish_node(renderer, 1, "open_source")
    refresh.assert_called_once_with()

    root, order_records = progress.tasks
    assert root.description == "stream:adv.20"
    assert root.total is None
    assert root.fields["status"] == ""
    assert order_records.visible is True
    assert order_records.fields["status"] == "reading · 100 records"


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
    root, order_records, map_records, open_source = progress.tasks
    assert root.fields["status"] == ""
    assert open_source.visible is True
    assert open_source.fields["status"] == '2/17 "2011.jsonl" · 20,000 items'

    renderer.handle(
        _node_progress(1, "map_records", ProgressSnapshot(completed=20_000))
    )
    assert open_source.visible is True
    assert map_records.visible is False

    renderer.handle(
        _node_progress(
            0,
            "order_records",
            ProgressSnapshot(completed=25, total=100, phase="merging"),
        )
    )
    assert open_source.visible is False
    assert order_records.visible is True
    assert order_records.completed == 25
    assert order_records.total == 100
    assert order_records.fields["status"] == "merging · 25/100 items"

    renderer.handle(
        _node_progress(
            1,
            "map_records",
            ProgressSnapshot(completed=20_000),
            heartbeat=True,
        )
    )

    assert order_records.visible is False
    assert map_records.visible is True
    assert map_records.fields["status"] == "20,000 items"


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
    root, order_records = progress.tasks
    label = _ProgressLabelColumn(Column()).render(root)
    assert label.plain == "[stream:adv.20] 0:00:10"
    assert label.spans[0].style == "dim"
    assert _NodeBarColumn(Column()).render(root).plain == ""
    assert root.description == "stream:adv.20"
    assert root.total is None
    assert root.completed == 0
    assert order_records.total == 100
    assert order_records.completed == 100

    now = 20.0
    renderer.handle(
        _node_progress(
            0,
            "order_records",
            ProgressSnapshot(completed=20, total=100, phase="emitting"),
        )
    )

    root, order_records = progress.tasks
    assert _ProgressLabelColumn(Column()).render(root).plain == (
        "[stream:adv.20] 0:00:20"
    )
    assert root.description == "stream:adv.20"
    assert root.total is None
    assert root.completed == 0
    assert order_records.completed == 20


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
            task.fields["is_pipeline"],
            task.fields["status"],
        )
        for task in tasks
    ] == [
        ("stream:adv.20", True, ""),
        ("stream:adv.20/order_records", False, "0 out"),
        ("stream:adv.20/open_source", False, "25/100 records"),
        ("stream:adv.20/decode_records", False, "0 out"),
    ]
    root_task, _, source_task, _ = tasks
    label = _ProgressLabelColumn(Column())
    bar_column = _NodeBarColumn(Column())
    assert label.render(root_task).plain == "[stream:adv.20] 0:00:00"
    assert bar_column.render(root_task).plain == ""
    assert source_task.completed == 25
    assert source_task.total == 100
    bar = bar_column.render(source_task)
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

    with patch.object(progress, "refresh", wraps=progress.refresh) as refresh:
        renderer.handle(
            PipelineFinished(
                pipeline_name="stream:adv.20",
                node_count=4,
                status="success",
                output_items=100,
                elapsed_seconds=1,
            )
        )
    refresh.assert_called_once_with()

    assert progress.tasks == []


def test_rich_renderer_routes_node_progress_without_printing() -> None:
    console, output = _console()
    progress_renderer = _CaptureRenderer()
    renderer = _RichExecutionRenderer(logging.INFO, console, progress_renderer)
    event = _node_progress(0, "open_source", ProgressSnapshot(completed=10))

    renderer.render(event)

    assert progress_renderer.events == [event]
    assert output.getvalue() == ""


def test_rich_renderer_does_not_print_pipeline_heartbeat_over_live_progress() -> None:
    console, output = _console()
    progress_renderer = _CaptureRenderer()
    renderer = _RichExecutionRenderer(logging.INFO, console, progress_renderer)

    renderer.render(
        PipelineProgress(
            pipeline_name="stream:adv.20",
            output_items=100,
            elapsed_seconds=60,
        )
    )

    assert progress_renderer.events == []
    assert output.getvalue() == ""


def test_rich_renderer_persists_node_finish_at_debug() -> None:
    console, output = _console(width=120)
    progress_renderer = _CaptureRenderer()
    renderer = _RichExecutionRenderer(logging.DEBUG, console, progress_renderer)
    event = NodeFinished(
        pipeline_name="stream:adv.20",
        node_name="rolling",
        node_index=3,
        status="success",
        output_items=100,
        elapsed_seconds=1,
    )

    renderer.render(event)

    assert progress_renderer.events == [event]
    assert output.getvalue().strip() == (
        "[stream:adv.20/rolling] finished status=success out=100 elapsed=1.000000s"
    )


def test_rich_renderer_hides_successful_node_finish_at_info() -> None:
    console, output = _console()
    progress_renderer = _CaptureRenderer()
    renderer = _RichExecutionRenderer(logging.INFO, console, progress_renderer)
    event = NodeFinished(
        pipeline_name="stream:adv.20",
        node_name="rolling",
        node_index=3,
        status="success",
        output_items=100,
        elapsed_seconds=1,
    )

    renderer.render(event)

    assert progress_renderer.events == [event]
    assert output.getvalue() == ""


def test_rich_renderer_persists_failed_node_finish_at_info() -> None:
    console, output = _console(width=140)
    progress_renderer = _CaptureRenderer()
    renderer = _RichExecutionRenderer(logging.INFO, console, progress_renderer)
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

    renderer.render(event)

    assert progress_renderer.events == [event]
    assert output.getvalue().strip() == (
        "[stream:adv.20/rolling] finished "
        "status=error error=ValueError: invalid record out=0 elapsed=1.000000s"
    )


def test_rich_renderer_persists_root_pipeline_lifecycle() -> None:
    console, output = _console()
    progress_renderer = _CaptureRenderer()
    renderer = _RichExecutionRenderer(logging.INFO, console, progress_renderer)
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
        renderer.render(event)

    assert progress_renderer.events == events
    assert output.getvalue().splitlines() == [
        "[stream:adv.20] started nodes=4",
        "[stream:adv.20] finished status=success items=100 elapsed=1.000000s",
    ]


def test_rich_renderer_static_events_are_flat() -> None:
    console, output = _console()
    renderer = _RichExecutionRenderer(logging.INFO, console)

    renderer.render(ExecutionMessage(message="Saved 10 records"))

    assert output.getvalue().splitlines() == ["Saved 10 records"]


def test_rich_renderer_hides_debug_config_at_info() -> None:
    console, output = _console()
    renderer = _RichExecutionRenderer(logging.INFO, console)

    renderer.render(
        ExecutionMessage(
            message="Config:\n{}",
            log_level=logging.DEBUG,
        )
    )

    assert output.getvalue() == ""


def test_rich_renderer_renders_operation_sequence_once_and_in_order() -> None:
    console, output = _console()
    renderer = _RichExecutionRenderer(logging.DEBUG, console)

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
        renderer.render(event)

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


def test_rich_renderer_renders_minimal_operation_header_at_info(monkeypatch) -> None:
    console, _ = _console()
    renderer = _RichExecutionRenderer(logging.INFO, console)
    renderables = []
    monkeypatch.setattr(console, "print", renderables.append)

    renderer.render(OperationStarted("materialize:adv.20"))

    assert len(renderables) == 1
    segments = list(console.render(renderables[0], console.options))
    title = next(segment for segment in segments if "Operation" in segment.text)
    rules = [segment for segment in segments if "─" in segment.text]
    assert title.text == "Operation materialize:adv.20"
    assert str(title.style) == "none"
    assert rules and all(
        segment.style is not None and segment.style.dim for segment in rules
    )


def test_rich_renderer_hides_operation_header_at_warning() -> None:
    console, output = _console()
    renderer = _RichExecutionRenderer(logging.WARNING, console)

    renderer.render(OperationStarted("materialize:adv.20"))

    assert output.getvalue() == ""


def test_rich_renderer_styles_only_final_status() -> None:
    console, _ = _console()
    renderer = _RichExecutionRenderer(logging.INFO, console)
    success = renderer._render_event(
        PipelineFinished(
            pipeline_name="stream:adv.20",
            node_count=4,
            status="success",
            output_items=100,
            elapsed_seconds=1,
        )
    )
    operation_error = renderer._render_event(
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


def test_rich_renderer_renders_debug_config_as_regular_text() -> None:
    console, _ = _console()
    renderer = _RichExecutionRenderer(logging.DEBUG, console)

    text = renderer._render_event(
        ExecutionMessage(message="Config:\n{}", log_level=logging.DEBUG)
    )

    assert text.style == ""


def test_rich_renderer_renders_file_result_as_aligned_link() -> None:
    console, output = _console(width=60)
    path = Path(
        "/Users/anders/project/artifacts/smoke/processed/very-long/"
        "dataset/dataset.train_0.jsonl"
    )
    event = FileResult(label="train_0", path=path)
    renderer = _RichExecutionRenderer(logging.INFO, console)

    renderer.render(event)

    lines = output.getvalue().splitlines()
    assert lines[0].startswith("train_0: ")
    assert lines[1].startswith(" " * 9)

    table = renderer._render_file_result(event)
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

    def previous_event_handler(_event) -> None:
        pass

    def previous_log_handler(_event) -> None:
        pass

    event_token = set_current_execution_event_handler(previous_event_handler)
    log_token = set_current_terminal_log_handler(previous_log_handler)
    try:
        with visual_execution(logging.INFO):
            assert current_execution_event_handler() is not previous_event_handler
            assert current_terminal_log_handler() is not previous_log_handler
        assert current_execution_event_handler() is previous_event_handler
        assert current_terminal_log_handler() is previous_log_handler
    finally:
        reset_current_terminal_log_handler(log_token)
        reset_current_execution_event_handler(event_token)


def test_visual_execution_releases_file_proxies_before_process_shutdown() -> None:
    script = dedent(
        """
        import gc
        import logging
        import sys
        import weakref

        from rich.console import Console
        from rich.file_proxy import FileProxy

        import datapipeline.cli.visuals.rich.progress as progress

        progress.Console = lambda **kwargs: Console(
            file=kwargs["file"],
            markup=False,
            highlight=False,
            force_terminal=True,
        )
        stdout = sys.stdout
        stderr = sys.stderr
        with progress.visual_execution(logging.INFO):
            assert isinstance(sys.stderr, FileProxy)
            proxy = weakref.ref(sys.stderr)
            sys.stderr.write("buffered without newline")

        assert sys.stdout is stdout
        assert sys.stderr is stderr
        gc.collect()
        assert proxy() is None
        """
    )

    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr


def test_rich_visuals_require_a_supported_tty(monkeypatch) -> None:
    monkeypatch.setattr(
        rich_progress.sys,
        "stderr",
        SimpleNamespace(isatty=lambda: True),
    )
    monkeypatch.setattr(
        rich_progress,
        "Console",
        lambda **_kwargs: SimpleNamespace(
            is_terminal=True,
            is_interactive=True,
            is_dumb_terminal=False,
            color_system="truecolor",
        ),
    )

    assert rich_visuals_supported() is True


def test_rich_visuals_are_disabled_without_a_tty(monkeypatch) -> None:
    monkeypatch.setattr(
        rich_progress.sys,
        "stderr",
        SimpleNamespace(isatty=lambda: False),
    )

    def fail_console(**_kwargs):
        raise AssertionError("Console must not be inspected without a TTY")

    monkeypatch.setattr(rich_progress, "Console", fail_console)

    assert rich_visuals_supported() is False


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
    assert isinstance(label, _ProgressLabelColumn)
    assert isinstance(bar, _NodeBarColumn)
    assert bar._bar.complete_style == "cyan"
    assert bar._bar.finished_style == "cyan"
