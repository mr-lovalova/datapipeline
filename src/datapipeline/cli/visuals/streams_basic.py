from typing import Iterator, Any, Optional, Callable
from contextlib import contextmanager
from itertools import cycle
import logging
import threading
import time

from datapipeline.runtime import Runtime
from datapipeline.sources.models.source import Source
from tqdm import tqdm
from .common import (
    resolve_progress_style_mode,
)
from .source_observability import SourceObservabilityAdapter

logger = logging.getLogger(__name__)


class VisualSourceProxy(Source):
    """Proxy wrapping Source.stream() with CLI feedback scaled by logging level."""

    def __init__(self, stream_source: Source, stream_id: str, verbosity: int):
        self._inner = stream_source
        self._stream_id = stream_id
        self._verbosity = max(0, min(verbosity, 2))

    @staticmethod
    def _start_spinner(label: str):
        """Start a background spinner tqdm progress bar."""
        bar = tqdm(
            total=0,
            desc="",
            bar_format="{desc}",
            dynamic_ncols=True,
            leave=False,
        )
        state = {"base": label}
        bar.set_description_str(label)
        bar.refresh()

        stop_event = threading.Event()

        def _spin():
            frames = cycle((" |", " /", " -", " \\"))
            while not stop_event.is_set():
                bar.set_description_str(f"{state['base']}{next(frames)}")
                bar.refresh()
                time.sleep(0.1)
            bar.set_description_str(state["base"])
            bar.refresh()

        worker = threading.Thread(target=_spin, daemon=True)
        worker.start()
        return state, stop_event, worker, bar

    @staticmethod
    def _stop_spinner(stop_event, worker, bar):
        stop_event.set()
        worker.join()
        try:
            bar.close()
        finally:
            fp = getattr(bar, "fp", None)
            try:
                if getattr(bar, "disable", False):
                    return
                if fp and hasattr(fp, "write"):
                    fp.write("\n")
                    fp.flush()
                else:
                    print()
            except Exception:
                pass

    def _count_with_indicator(self, label: str, count_fn: Callable[[], Optional[int]]) -> Optional[int]:
        try:
            _, stop_event, worker, bar = self._start_spinner(label)
        except Exception:
            # If spinner setup fails, silently fall back to raw count
            return count_fn()

        try:
            return count_fn()
        finally:
            self._stop_spinner(stop_event, worker, bar)

    def stream(self) -> Iterator[Any]:
        adapter = SourceObservabilityAdapter(self._inner, self._stream_id)
        label = adapter.preparing_label()

        last_path_label: Optional[str] = None

        def maybe_update_label(apply_label):
            nonlocal last_path_label
            current_label = adapter.current_label()
            if not current_label or current_label == last_path_label:
                return
            last_path_label = current_label
            apply_label(current_label)

        def log_start_details() -> None:
            indent = adapter.current_indent()
            adapter.log_composed_details()
            if logger.isEnabledFor(logging.INFO):
                for line in adapter.info_lines():
                    logger.info("%s[%s] %s", indent, self._stream_id, line)
            if logger.isEnabledFor(logging.DEBUG):
                for line in adapter.debug_lines():
                    logger.debug("%s[%s] %s", indent, self._stream_id, line)

        emitted = 0
        if self._verbosity >= 2:
            total = self._count_with_indicator(label, adapter.count)

            bar = tqdm(
                total=total,
                desc=adapter.format_label(),
                unit=adapter.unit,
                dynamic_ncols=True,
                mininterval=0.0,
                miniters=1,
                leave=True,
            )

            started = False

            def update_progress(name: str) -> None:
                bar.set_description_str(adapter.format_label(name))
                bar.refresh()

            try:
                for item in self._inner.stream():
                    if not started:
                        # Emit transport details on first item for correct ordering (DEBUG verbosity)
                        log_start_details()
                        started = True
                    maybe_update_label(update_progress)
                    bar.update()
                    emitted += 1
                    yield item
            finally:
                bar.close()
                if logger.isEnabledFor(logging.INFO):
                    try:
                        indent = adapter.current_indent()
                        unit_label = f" {adapter.unit}" if adapter.unit else ""
                        logger.info("%s[%s] Stream complete (%d%s) ✔",
                                    indent, self._stream_id, emitted, unit_label)
                    except Exception:
                        pass
            return

        try:
            state, stop_event, worker, bar = self._start_spinner(
                adapter.format_label())
        except Exception:
            # Spinner isn't critical; fall back to raw stream
            yield from self._inner.stream()
            return

        def update_spinner(name: str) -> None:
            state["base"] = adapter.format_label(name)
            bar.set_description_str(state["base"])
            bar.refresh()

        started = False
        try:
            for item in self._inner.stream():
                if not started:
                    # Emit transport details at the start for correct grouping
                    log_start_details()
                    started = True
                maybe_update_label(update_spinner)
                emitted += 1
                yield item
        finally:
            self._stop_spinner(stop_event, worker, bar)
            if logger.isEnabledFor(logging.INFO):
                try:
                    indent = adapter.current_indent()
                    unit_label = f" {adapter.unit}" if adapter.unit else ""
                    logger.info("%s[%s] Stream complete (%d%s) ✔",
                                indent, self._stream_id, emitted, unit_label)
                except Exception:
                    pass


@contextmanager
def visual_sources(runtime: Runtime, log_level: int | None, progress_style: str = "auto"):
    """Temporarily wrap stream sources with logging-level-driven feedback."""
    level = log_level if log_level is not None else logging.INFO
    style_mode = resolve_progress_style_mode(progress_style, log_level)
    if style_mode == "off" or level > logging.INFO:
        yield
        return

    verbosity = 2 if style_mode == "bars" else 1

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())
    try:
        for stream_id, stream_source in originals.items():
            reg.register(
                stream_id,
                VisualSourceProxy(stream_source, stream_id, verbosity),
            )
        yield
    finally:
        # Restore original sources
        for stream_id, stream_source in originals.items():
            reg.register(stream_id, stream_source)
