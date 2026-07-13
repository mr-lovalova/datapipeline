import threading
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path

import pytest

from datapipeline.execution import runner as pipeline_runner
from datapipeline.execution.context import PipelineContext
from datapipeline.execution.pipeline import Pipeline
from datapipeline.execution.events import (
    PipelineRunEvent,
    NodeExecutionEvent,
    NodeProgressEvent,
    ProgressSnapshot,
)
from datapipeline.execution.node import PipelineNode, SourceNode
from datapipeline.execution.runner import run_pipeline
from datapipeline.runtime import Runtime


class _CollectingObserver:
    def __init__(self) -> None:
        self.pipeline_started: list[tuple[str, int, str | None]] = []
        self.node_started: list[tuple[str, str, int]] = []
        self.node_events: list[NodeExecutionEvent] = []
        self.progress_events: list[NodeProgressEvent] = []
        self.pipeline_events: list[PipelineRunEvent] = []
        self._progress_condition = threading.Condition()

    def on_pipeline_start(
        self,
        pipeline_name: str,
        node_count: int,
        summary: str | None = None,
    ) -> None:
        self.pipeline_started.append((pipeline_name, node_count, summary))

    def on_node_start(
        self,
        pipeline_name: str,
        node_name: str,
        node_index: int,
    ) -> None:
        self.node_started.append((pipeline_name, node_name, node_index))

    def on_node_end(self, event: NodeExecutionEvent) -> None:
        self.node_events.append(event)

    def on_node_progress(self, event: NodeProgressEvent) -> None:
        with self._progress_condition:
            self.progress_events.append(event)
            self._progress_condition.notify_all()

    def on_pipeline_end(self, event: PipelineRunEvent) -> None:
        self.pipeline_events.append(event)

    def wait_for_progress(
        self,
        predicate: Callable[[NodeProgressEvent], bool],
    ) -> bool:
        with self._progress_condition:
            return self._progress_condition.wait_for(
                lambda: any(predicate(event) for event in self.progress_events),
                timeout=1.0,
            )


def _context(tmp_path: Path) -> PipelineContext:
    project_yaml = tmp_path / "project.yaml"
    project_yaml.write_text("version: 1\n", encoding="utf-8")
    artifacts_root = tmp_path / "artifacts"
    artifacts_root.mkdir(parents=True, exist_ok=True)
    return PipelineContext(
        Runtime(project_yaml=project_yaml, artifacts_root=artifacts_root)
    )


def _events_by_name(
    observer: _CollectingObserver,
) -> dict[str, NodeExecutionEvent]:
    return {event.node_name: event for event in observer.node_events}


def test_pipeline_requires_unique_names_and_a_first_source() -> None:
    with pytest.raises(ValueError, match="node names must be unique"):
        Pipeline(
            name="duplicate",
            nodes=(
                SourceNode("records", lambda: ()),
                PipelineNode("records", lambda records: records),
            ),
        )

    with pytest.raises(ValueError, match="source node 'records' must be first"):
        Pipeline(
            name="late-source",
            nodes=(
                PipelineNode("transform", lambda records: records),
                SourceNode("records", lambda: ()),
            ),
        )


def test_pipeline_can_stop_at_an_ordered_stage() -> None:
    pipeline = Pipeline(
        name="linear",
        nodes=(
            SourceNode("source", lambda: ()),
            PipelineNode("map", lambda records: records),
            PipelineNode("filter", lambda records: records),
        ),
        summary="three stages",
    )

    assert pipeline.node_count == 3
    assert [node.name for node in pipeline.through_node(1).nodes] == ["source", "map"]
    assert [node.name for node in pipeline.through_node_named("filter").nodes] == [
        "source",
        "map",
        "filter",
    ]
    with pytest.raises(ValueError, match="no node named 'missing'"):
        pipeline.through_node_named("missing")
    with pytest.raises(ValueError, match="node index -1 is out of range"):
        pipeline.through_node(-1)


def test_run_starts_lazily(tmp_path: Path) -> None:
    opened: list[str] = []
    observer = _CollectingObserver()

    def open_records() -> Iterator[int]:
        opened.append("source")
        yield 1

    stream = run_pipeline(
        _context(tmp_path),
        Pipeline(name="lazy", nodes=(SourceNode("source", open_records),)),
        observer=observer,
    )

    assert opened == []
    assert observer.pipeline_started == []
    assert next(stream) == 1
    assert opened == ["source"]
    assert observer.pipeline_started == [("lazy", 1, None)]
    stream.close()


def test_source_pipeline_rejects_a_seed(tmp_path: Path) -> None:
    pipeline = Pipeline(name="source", nodes=(SourceNode("source", lambda: [1]),))

    with pytest.raises(ValueError, match="cannot use both a source and a seed"):
        list(run_pipeline(_context(tmp_path), pipeline, seed=[2]))


def test_stage_pipeline_requires_and_consumes_a_seed(tmp_path: Path) -> None:
    pipeline = Pipeline(
        name="seeded",
        nodes=(
            PipelineNode("double", lambda records: (value * 2 for value in records)),
        ),
    )

    with pytest.raises(ValueError, match="requires a source or a seed"):
        list(run_pipeline(_context(tmp_path), pipeline))

    assert list(run_pipeline(_context(tmp_path), pipeline, seed=[1, 2])) == [2, 4]


def test_stages_emit_ordered_results_and_counts(tmp_path: Path) -> None:
    observer = _CollectingObserver()

    def odd(records: Iterable[int]) -> Iterator[int]:
        for value in records:
            if value % 2:
                yield value

    pipeline = Pipeline(
        name="numbers",
        summary="filter then scale",
        nodes=(
            SourceNode("source", lambda: [1, 2, 3]),
            PipelineNode("odd", odd),
            PipelineNode("scale", lambda records: (value * 10 for value in records)),
        ),
    )

    assert list(run_pipeline(_context(tmp_path), pipeline, observer=observer)) == [
        10,
        30,
    ]

    events = _events_by_name(observer)
    assert {
        name: (event.node_index, event.output_items, event.status)
        for name, event in events.items()
    } == {
        "source": (0, 3, "success"),
        "odd": (1, 2, "success"),
        "scale": (2, 2, "success"),
    }
    assert observer.pipeline_started == [("numbers", 3, "filter then scale")]
    assert observer.pipeline_events[-1].output_items == 2
    assert observer.pipeline_events[-1].status == "success"


def test_custom_progress_belongs_to_the_reporting_stage(tmp_path: Path) -> None:
    observer = _CollectingObserver()

    def annotate(records: Iterable[int]) -> Iterator[int]:
        for value in records:
            pipeline_runner.report_node_progress(
                ProgressSnapshot(
                    completed=value,
                    total=2,
                    phase="mapping",
                    detail="after input read",
                )
            )
            yield value

    pipeline = Pipeline(
        name="progress",
        nodes=(
            SourceNode("source", lambda: [1, 2]),
            PipelineNode("annotate", annotate),
        ),
    )

    assert list(run_pipeline(_context(tmp_path), pipeline, observer=observer)) == [1, 2]

    custom = [
        event
        for event in observer.progress_events
        if event.progress.detail == "after input read"
    ]
    assert [event.progress.completed for event in custom] == [1, 2]
    assert all(event.node_name == "annotate" for event in custom)
    assert all(event.node_index == 1 for event in custom)
    assert all(not event.persistent for event in custom)


def test_live_progress_samples_emitted_items(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline_runner, "_LIVE_PROGRESS_INTERVAL_SECONDS", 0.001)
    observer = _CollectingObserver()
    context = _context(tmp_path)
    context.heartbeat_interval_seconds = 0

    def source() -> Iterator[int]:
        yield 1
        assert observer.wait_for_progress(
            lambda event: (
                event.node_name == "source"
                and event.progress.completed >= 1
                and not event.persistent
            )
        )
        yield 2

    pipeline = Pipeline(name="sampled", nodes=(SourceNode("source", source),))

    assert list(run_pipeline(context, pipeline, observer=observer)) == [1, 2]


def test_heartbeat_reports_current_item_count(tmp_path: Path) -> None:
    observer = _CollectingObserver()
    context = _context(tmp_path)
    context.heartbeat_interval_seconds = 0.01

    def source() -> Iterator[int]:
        assert observer.wait_for_progress(
            lambda event: event.persistent and event.progress.completed == 0
        )
        yield 1
        assert observer.wait_for_progress(
            lambda event: event.persistent and event.progress.completed >= 1
        )
        yield 2

    pipeline = Pipeline(name="heartbeat", nodes=(SourceNode("source", source),))

    assert list(run_pipeline(context, pipeline, observer=observer)) == [1, 2]
    heartbeats = [event for event in observer.progress_events if event.persistent]
    assert heartbeats[0].node_name == "source"
    assert heartbeats[0].progress.completed == 0
    assert heartbeats[-1].progress.completed == 1


def test_partial_close_closes_stages_in_reverse_order(tmp_path: Path) -> None:
    closed: list[str] = []
    observer = _CollectingObserver()

    def source() -> Iterator[int]:
        try:
            yield 1
            yield 2
        finally:
            closed.append("source")

    def closing_stage(
        records: Iterable[int],
        name: str,
    ) -> Iterator[int]:
        try:
            for record in records:
                yield record
        finally:
            closed.append(name)

    pipeline = Pipeline(
        name="closing",
        nodes=(
            SourceNode("source", source),
            PipelineNode("first", lambda records: closing_stage(records, "first")),
            PipelineNode("second", lambda records: closing_stage(records, "second")),
        ),
    )

    stream = run_pipeline(_context(tmp_path), pipeline, observer=observer)
    assert next(stream) == 1
    stream.close()

    assert closed == ["second", "first", "source"]
    assert {
        event.node_name: (event.output_items, event.status)
        for event in observer.node_events
    } == {
        "source": (1, "success"),
        "first": (1, "success"),
        "second": (1, "success"),
    }
    assert observer.pipeline_events[-1].output_items == 1


@pytest.mark.parametrize(
    ("seed", "expected"),
    [
        (None, []),
        ([1, 2], [1, 2]),
    ],
)
def test_empty_pipeline_is_a_valid_boundary(
    tmp_path: Path,
    seed: list[int] | None,
    expected: list[int],
) -> None:
    observer = _CollectingObserver()

    output = list(
        run_pipeline(
            _context(tmp_path),
            Pipeline(name="empty", nodes=()),
            seed=seed,
            observer=observer,
        )
    )

    assert output == expected
    assert observer.node_started == []
    assert observer.node_events == []
    assert observer.pipeline_events[-1].output_items == len(expected)


@pytest.mark.parametrize("source_node", [True, False])
def test_none_node_output_is_rejected(
    tmp_path: Path,
    source_node: bool,
) -> None:
    observer = _CollectingObserver()
    if source_node:
        pipeline = Pipeline(
            name="none",
            nodes=(SourceNode("broken", lambda: None),),  # type: ignore[arg-type]
        )
        seed = None
    else:
        pipeline = Pipeline(
            name="none",
            nodes=(
                PipelineNode("broken", lambda records: None),  # type: ignore[arg-type]
            ),
        )
        seed = [1]

    with pytest.raises(
        TypeError,
        match="Pipeline node 'broken' returned None; return an iterable",
    ):
        list(run_pipeline(_context(tmp_path), pipeline, seed=seed, observer=observer))

    event = observer.node_events[-1]
    assert event.node_name == "broken"
    assert event.status == "error"
    assert event.error_type == "TypeError"
    assert observer.pipeline_events[-1].status == "error"


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (RuntimeError("boom"), "boom"),
        (KeyboardInterrupt(), None),
    ],
)
def test_stage_failures_reach_node_and_pipeline_events(
    tmp_path: Path,
    error: BaseException,
    message: str | None,
) -> None:
    observer = _CollectingObserver()

    def fail(records: Iterable[int]) -> Iterator[int]:
        for value in records:
            if value == 2:
                raise error
            yield value

    pipeline = Pipeline(
        name="failure",
        nodes=(
            SourceNode("source", lambda: [1, 2, 3]),
            PipelineNode("fail", fail),
        ),
    )

    with pytest.raises(type(error)):
        list(run_pipeline(_context(tmp_path), pipeline, observer=observer))

    node_event = _events_by_name(observer)["fail"]
    assert node_event.output_items == 1
    assert node_event.status == "error"
    assert node_event.error_type == type(error).__name__
    assert node_event.error_message == message
    pipeline_event = observer.pipeline_events[-1]
    assert pipeline_event.output_items == 1
    assert pipeline_event.status == "error"
    assert pipeline_event.error_type == type(error).__name__
    assert pipeline_event.error_message == message


def test_unobserved_run_uses_the_fast_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_progress_start(_progress: object) -> None:
        raise AssertionError("unobserved pipelines must not start progress machinery")

    monkeypatch.setattr(
        pipeline_runner._RunProgress, "start", unexpected_progress_start
    )
    pipeline = Pipeline(
        name="fast",
        nodes=(
            SourceNode("source", lambda: [1, 2]),
            PipelineNode("double", lambda records: (value * 2 for value in records)),
        ),
    )

    assert list(run_pipeline(_context(tmp_path), pipeline)) == [2, 4]


def test_resolve_heartbeat_interval_accepts_supported_values() -> None:
    assert (
        pipeline_runner.resolve_heartbeat_interval_seconds(None)
        == pipeline_runner.DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    )
    assert pipeline_runner.resolve_heartbeat_interval_seconds(0) == 0
    assert (
        pipeline_runner.resolve_heartbeat_interval_seconds(threading.TIMEOUT_MAX)
        == threading.TIMEOUT_MAX
    )


@pytest.mark.parametrize(
    ("interval", "message"),
    [
        (-1, "non-negative"),
        (float("nan"), "finite"),
        (float("inf"), "finite"),
        (threading.TIMEOUT_MAX + 1, "must not exceed"),
    ],
)
def test_resolve_heartbeat_interval_rejects_invalid_values(
    interval: float,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        pipeline_runner.resolve_heartbeat_interval_seconds(interval)
