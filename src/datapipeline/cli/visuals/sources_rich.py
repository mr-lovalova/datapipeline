from contextlib import contextmanager
from typing import Iterator, Any, Optional, Deque, Dict, Tuple
from pathlib import Path
from math import ceil
import logging
import os

from collections import deque

from rich.live import Live
from rich.progress import (
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    MofNCompleteColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    Task,
)
from rich.text import Text

from .labels import progress_meta_for_loader
from .common import (
    compute_glob_root,
    current_transport_label,
    log_combined_stream,
    transport_debug_lines,
    transport_info_lines,
)
from datapipeline.runtime import Runtime
from datapipeline.sources.models.source import Source
from datapipeline.sources.transports import FsGlobTransport, FsFileTransport, HttpTransport
logger = logging.getLogger(__name__)


class AverageTimeRemainingColumn(ProgressColumn):
    """ETA column that blends long-term and recent throughput for stability."""

    max_refresh = 0.5

    def __init__(
        self,
        compact: bool = False,
        elapsed_when_finished: bool = False,
        table_column: Optional[Any] = None,
        window_seconds: float = 300.0,
    ) -> None:
        self.compact = compact
        self.elapsed_when_finished = elapsed_when_finished
        self.window_seconds = max(0.0, float(window_seconds))
        self._history: Dict[int, Deque[Tuple[float, float]]] = {}
        super().__init__(table_column=table_column)

    def _format_seconds(self, seconds: int) -> str:
        minutes, secs = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if self.compact and not hours:
            return f"{minutes:02d}:{secs:02d}"
        return f"{hours:d}:{minutes:02d}:{secs:02d}"

    def _recent_seconds_per_item(self, task: Task) -> Optional[float]:
        if self.window_seconds <= 0:
            return None
        if task.start_time is None:
            return None
        history = self._history.setdefault(int(task.id), deque())
        now = task.get_time()
        completed = float(task.completed)
        if not history or history[-1][1] != completed:
            history.append((now, completed))
        cutoff = now - self.window_seconds
        while history and history[0][0] < cutoff:
            history.popleft()
        if len(history) < 2:
            return None
        start_time, start_completed = history[0]
        delta_completed = completed - start_completed
        delta_time = now - start_time
        if delta_completed <= 0 or delta_time <= 0:
            return None
        return delta_time / delta_completed

    def render(self, task: Task) -> Text:
        if self.elapsed_when_finished and task.finished:
            self._history.pop(int(task.id), None)
            elapsed = task.finished_time
            if elapsed is None:
                return Text("-:--:--", style="progress.elapsed")
            return Text(self._format_seconds(int(elapsed)), style="progress.elapsed")

        style = "progress.remaining"
        total = task.total
        if total is None:
            return Text("", style=style)
        elapsed = task.elapsed
        completed = task.completed
        remaining = task.remaining
        if not completed or elapsed is None or remaining is None:
            return Text("--:--" if self.compact else "-:--:--", style=style)
        recent = self._recent_seconds_per_item(task)
        avg_seconds_per_item = recent if recent is not None else (elapsed / completed)
        if avg_seconds_per_item <= 0:
            return Text("--:--" if self.compact else "-:--:--", style=style)
        eta_seconds = int(max(0, ceil(remaining * avg_seconds_per_item)))
        return Text(self._format_seconds(eta_seconds), style=style)


class _RichSourceProxy(Source):
    def __init__(self, *, inner: Source, alias: str, verbosity: int, progress: Progress, unit: Optional[str] = None, shared_task_id: Optional[int] = None, finalize: Optional[callable] = None, started: Optional[callable] = None):
        self._inner = inner
        self._alias = alias
        self._verbosity = max(0, min(verbosity, 2))
        self._progress = progress
        self._task_id = None
        self._shared_task_id = shared_task_id
        self._unit = unit
        self._emitted = 0
        self._finalize = finalize
        self._started = started

    def _format_text(self, message: str) -> str:
        # Plain alias prefix to avoid Rich markup issues
        return f"[{self._alias}] {message}" if message else f"[{self._alias}]"

    def _safe_count(self) -> Optional[int]:
        try:
            return self._inner.count()
        except Exception:
            return None

    def stream(self) -> Iterator[Any]:
        loader = getattr(self._inner, "loader", None)
        desc, unit = progress_meta_for_loader(loader)
        self._unit = unit
        prefix, sep, suffix = desc.partition(": ")
        header = f"{prefix}:" if sep else desc
        tail = suffix if sep else None

        transport = getattr(loader, "transport", None)
        glob_root: Optional[Path] = None
        if isinstance(transport, FsGlobTransport):
            glob_root = compute_glob_root(
                getattr(transport, "files", []))

        def compose_text(name: Optional[str]) -> str:
            if name:
                base = header if sep else desc
                return f"{base} {name}".rstrip()
            if tail:
                return f"{header} {tail}".rstrip()
            return f"{desc}"

        # Create task lazily with no total (DEBUG) or reuse shared spinner (INFO)
        if self._verbosity >= 2 or self._shared_task_id is None:
            self._task_id = self._progress.add_task(
                "", start=False, total=None, text=self._format_text(compose_text(None)))

        # If verbose, try to resolve total and show a real bar
        if self._verbosity >= 2 and self._task_id is not None:
            total = self._safe_count()
            if total is not None:
                self._progress.update(self._task_id, total=total)

        emitted = 0
        last_path_label: Optional[str] = None
        shared_init_done = False
        started_logged = False

        if self._task_id is not None:
            self._progress.start_task(self._task_id)

        try:
            for item in self._inner.stream():
                current_label = current_transport_label(
                    transport, glob_root=glob_root
                )
                # On first item: emit Start + transport details
                if not started_logged:
                    try:
                        if callable(self._started):
                            info_lines = transport_info_lines(transport)
                            debug_lines = transport_debug_lines(
                                transport) if self._verbosity >= 2 else []
                            self._started(self._alias, info_lines, debug_lines)
                    except Exception:
                        pass
                    started_logged = True
                # Initialize shared spinner text on first item (INFO)
                if not shared_init_done and self._shared_task_id is not None:
                    base = current_label if current_label else None
                    text0 = self._format_text(compose_text(base))
                    self._progress.update(self._shared_task_id, text=text0)
                    shared_init_done = True
                if current_label and current_label != last_path_label:
                    last_path_label = current_label
                    text = self._format_text(compose_text(current_label))
                    if self._verbosity >= 2 and self._task_id is not None:
                        self._progress.update(self._task_id, text=text)
                    elif self._shared_task_id is not None:
                        self._progress.update(self._shared_task_id, text=text)
                if self._verbosity >= 2 and self._task_id is not None:
                    self._progress.advance(self._task_id, 1)
                emitted += 1
                yield item
        finally:
            try:
                if self._verbosity >= 2 and self._task_id is not None:
                    self._progress.update(self._task_id, completed=emitted)
                    self._progress.stop_task(self._task_id)
                unit = self._unit or "item"
                unit_suffix = "" if emitted == 1 else "s"
                completed_text = f"[{self._alias}] Stream complete ({emitted} {unit}{unit_suffix})"
                if callable(self._finalize):
                    try:
                        self._finalize(self._alias, completed_text)
                    except Exception:
                        pass
            except Exception:
                pass
            # Defer logging of completion to the session footer to avoid interleaving
            self._emitted = emitted
            # No explicit end separator; completion line is sufficient


def _style_mode(progress_style: str, log_level: int | None) -> str:
    mode = (progress_style or "auto").lower()
    if mode == "auto":
        level = log_level if log_level is not None else logging.INFO
        return "bars" if level <= logging.DEBUG else "spinner"
    return mode


@contextmanager
def visual_sources(runtime: Runtime, log_level: int | None, progress_style: str = "auto"):
    level = log_level if log_level is not None else logging.INFO
    if level > logging.INFO:
        yield
        return

    style_mode = _style_mode(progress_style, log_level)
    if style_mode == "off":
        yield
        return

    verbosity = 2 if style_mode == "bars" else 1

    # Build a console on stderr for visuals/logs
    from rich.console import Console as _Console
    import sys as _sys
    _vis_console = _Console(file=_sys.stderr, markup=False,
                            highlight=False, soft_wrap=True)

    # Columns tuned by style; alias is embedded in text
    if verbosity >= 2:
        columns = [
            TextColumn("{task.fields[text]}", markup=False),
            BarColumn(),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            AverageTimeRemainingColumn(),
        ]
    else:
        columns = [
            TextColumn("{task.fields[text]}", markup=False),
            SpinnerColumn(spinner_name="runner"),
        ]

    # Keep Live output transient so the spinner/bars disappear once completed
    progress = Progress(*columns, transient=True, console=_vis_console)

    # Install a temporary Rich logging handler for clean log rendering during Live
    class _DedupFilter(logging.Filter):
        def __init__(self):
            super().__init__()
            self._last: tuple[int, str] | None = None

        # type: ignore[override]
        def filter(self, record: logging.LogRecord) -> bool:
            try:
                msg = record.getMessage()
            except Exception:
                msg = record.msg if isinstance(
                    record.msg, str) else str(record.msg)
            key = (record.levelno, msg)
            if self._last == key:
                return False
            self._last = key
            return True

    rich_handler = None
    root_logger = logging.getLogger()
    old_handlers = list(root_logger.handlers)
    old_filters = list(root_logger.filters)
    try:
        from rich.logging import RichHandler
        console = _vis_console
        rich_handler = RichHandler(
            console=console,
            show_time=False,
            show_level=False,
            show_path=False,
            markup=False,
            rich_tracebacks=False,
        )
    except Exception:
        rich_handler = None

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())
    proxies: dict[str, _RichSourceProxy] = {}

    # Swap handlers if RichHandler is available
    if rich_handler is not None:
        # Replace handlers with Rich and add a simple de-dup filter to avoid
        # double-rendered lines if another handler slips in.
        root_logger.handlers = [rich_handler]
        dedup = _DedupFilter()
        root_logger.addFilter(dedup)

    renderable = progress

    with Live(renderable, console=_vis_console, refresh_per_second=10, transient=True) as live:
        try:
            shared_task_id: Optional[int] = None
            active_alias: Optional[str] = None
            pending_starts: list[tuple[str, list[tuple[str, str]]]] = []
            seen_messages: set[str] = set()

            def _emit_entries(entries: list[tuple[str, str]]) -> None:
                for level, line in entries:
                    key = f"{level}:{line}"
                    if key in seen_messages:
                        continue
                    seen_messages.add(key)
                    if level == "debug":
                        logger.debug(line)
                    else:
                        logger.info(line)

            def _flush_next_start() -> None:
                nonlocal active_alias
                if active_alias is not None:
                    return
                while pending_starts:
                    next_alias, entries = pending_starts.pop(0)
                    if not entries:
                        continue
                    active_alias = next_alias
                    _emit_entries(entries)
                    break

            def _append_completed(alias: str, text: str):
                _emit_entries([("info", f"{text} ✔")])
                nonlocal active_alias
                if active_alias == alias:
                    active_alias = None
                _flush_next_start()

            def _append_started(alias: str, info_lines: list[str], debug_lines: list[str]):
                nonlocal active_alias
                entries: list[tuple[str, str]] = []
                for line in info_lines:
                    entries.append(("info", f"[{alias}] {line}"))
                for line in debug_lines:
                    entries.append(("debug", f"[{alias}] {line}"))
                if not entries:
                    entries = [("info", f"[{alias}] Stream starting")]
                if active_alias is None:
                    active_alias = alias
                    _emit_entries(entries)
                    return
                pending_starts.append((alias, entries))
            if verbosity < 2:
                shared_task_id = progress.add_task("", total=None, text="")
            for alias, src in originals.items():
                # Composed/virtual sources (no loader): attach header-only proxy to emit when streamed
                if getattr(src, "loader", None) is None:
                    class _ComposedHeaderProxy:
                        def __init__(self, inner, alias: str):
                            self._inner = inner
                            self._alias = alias

                        def stream(self):
                            detail_entries: Optional[list[str]] = None
                            try:
                                spec = getattr(self._inner, "_spec", None)
                                inputs = getattr(spec, "inputs", None)
                                if isinstance(inputs, (list, tuple)) and inputs:
                                    detail_entries = [str(item)
                                                      for item in inputs]
                            except Exception:
                                detail_entries = None
                            log_combined_stream(self._alias, detail_entries)
                            yield from self._inner.stream()

                    reg.register(alias, _ComposedHeaderProxy(src, alias))
                else:
                    proxy = _RichSourceProxy(inner=src, alias=alias, verbosity=verbosity, progress=progress,
                                             shared_task_id=shared_task_id, finalize=_append_completed, started=_append_started)
                    proxies[alias] = proxy
                    reg.register(alias, proxy)
            yield
        finally:
            # Restore original sources
            for alias, src in originals.items():
                reg.register(alias, src)
    # After Live finishes: restore logging handlers
    if rich_handler is not None:
        # Restore original handlers and filters
        root_logger.handlers = old_handlers
        root_logger.filters = old_filters
