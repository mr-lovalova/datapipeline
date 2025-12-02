from typing import Iterator, Any, Optional
from contextlib import contextmanager
from itertools import cycle
from pathlib import Path
import logging
import os
import threading
import time

from .labels import progress_meta_for_loader
from datapipeline.runtime import Runtime
from datapipeline.sources.models.source import Source
from datapipeline.sources.transports import FsGlobTransport
from tqdm import tqdm
from .common import (
    compute_glob_root,
    current_transport_label,
    log_combined_stream,
    log_transport_details,
)

logger = logging.getLogger(__name__)


class VisualSourceProxy(Source):
    """Proxy wrapping Source.stream() with CLI feedback scaled by logging level."""

    def __init__(self, inner: Source, alias: str, verbosity: int):
        self._inner = inner
        self._alias = alias
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

    def _count_with_indicator(self, label: str) -> Optional[int]:
        try:
            _, stop_event, worker, bar = self._start_spinner(label)
        except Exception:
            # If spinner setup fails, silently fall back to raw count
            return self._safe_count()

        try:
            return self._safe_count()
        finally:
            self._stop_spinner(stop_event, worker, bar)

    def _safe_count(self) -> Optional[int]:
        try:
            return self._inner.count()
        except Exception:
            return None

    def _log_source_details(self, transport, root: Optional[Path]) -> None:
        # Use visuals-agnostic helper so behavior matches rich/basic
        log_transport_details(transport, self._alias)

    def stream(self) -> Iterator[Any]:
        loader = getattr(self._inner, "loader", None)
        desc, unit = progress_meta_for_loader(loader)
        prefix, sep, suffix = desc.partition(": ")
        header = f"{prefix}:" if sep else desc
        tail = suffix if sep else None
        label = f"[{self._alias}] Preparing data stream"

        transport = getattr(loader, "transport", None)

        glob_root: Optional[Path] = None
        if isinstance(transport, FsGlobTransport):
            glob_root = compute_glob_root(transport.files)

        last_path_label: Optional[str] = None

        def compose_desc(name: Optional[str]) -> str:
            if name:
                base = header if sep else desc
                return f"[{self._alias}] {base} {name}".rstrip()
            if tail:
                return f"[{self._alias}] {header} {tail}".rstrip()
            return f"[{self._alias}] {desc}"

        def maybe_update_label(apply_label):
            nonlocal last_path_label
            current_label = current_transport_label(transport, glob_root=glob_root)
            if not current_label or current_label == last_path_label:
                return
            last_path_label = current_label
            apply_label(current_label)

        emitted = 0
        if self._verbosity >= 2:
            total = self._count_with_indicator(label)

            bar = tqdm(
                total=total,
                desc=compose_desc(None),
                unit=unit,
                dynamic_ncols=True,
                mininterval=0.0,
                miniters=1,
                leave=True,
            )

            started = False

            def update_progress(name: str) -> None:
                bar.set_description_str(compose_desc(name))
                bar.refresh()

            try:
                for item in self._inner.stream():
                    if not started:
                        # Emit transport details on first item for correct ordering (DEBUG verbosity)
                        self._log_source_details(transport, glob_root)
                        started = True
                    maybe_update_label(update_progress)
                    bar.update()
                    emitted += 1
                    yield item
            finally:
                bar.close()
                if logger.isEnabledFor(logging.INFO):
                    try:
                        unit_label = f" {unit}" if unit else ""
                        logger.info("[%s] Stream complete (%d%s) ✔",
                                    self._alias, emitted, unit_label)
                    except Exception:
                        pass
            return

        try:
            state, stop_event, worker, bar = self._start_spinner(
                compose_desc(None))
        except Exception:
            # Spinner isn't critical; fall back to raw stream
            yield from self._inner.stream()
            return

        def update_spinner(name: str) -> None:
            state["base"] = compose_desc(name)
            bar.set_description_str(state["base"])
            bar.refresh()

        started = False
        try:
            for item in self._inner.stream():
                if not started:
                    # Emit transport details at the start for correct grouping
                    self._log_source_details(transport, glob_root)
                    started = True
                maybe_update_label(update_spinner)
                emitted += 1
                yield item
        finally:
            self._stop_spinner(stop_event, worker, bar)
            if logger.isEnabledFor(logging.INFO):
                try:
                    unit_label = f" {unit}" if unit else ""
                    logger.info("[%s] Stream complete (%d%s) ✔",
                                self._alias, emitted, unit_label)
                except Exception:
                    pass


def _style_mode(progress_style: str, log_level: int | None) -> str:
    mode = (progress_style or "auto").lower()
    if mode == "auto":
        level = log_level if log_level is not None else logging.INFO
        return "bars" if level <= logging.DEBUG else "spinner"
    return mode


@contextmanager
def visual_sources(runtime: Runtime, log_level: int | None, progress_style: str = "auto"):
    """Temporarily wrap stream sources with logging-level-driven feedback."""
    level = log_level if log_level is not None else logging.INFO
    style_mode = _style_mode(progress_style, log_level)
    if style_mode == "off" or level > logging.INFO:
        yield
        return

    verbosity = 2 if style_mode == "bars" else 1

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())
    try:
        # Lightweight proxy that only prints a composed header when actually streamed
        class _ComposedHeaderProxy:
            def __init__(self, inner, alias: str):
                self._inner = inner
                self._alias = alias

            def stream(self):  # Iterator[Any]
                detail_entries: Optional[list[str]] = None
                try:
                    spec = getattr(self._inner, "_spec", None)
                    inputs = getattr(spec, "inputs", None)
                    if isinstance(inputs, (list, tuple)) and inputs:
                        detail_entries = [str(item) for item in inputs]
                except Exception:
                    detail_entries = None
                log_combined_stream(self._alias, detail_entries)
                yield from self._inner.stream()

        for alias, src in originals.items():
            # Wrap composed/virtual sources with a header-only proxy; others with visuals
            if getattr(src, "loader", None) is None:
                reg.register(alias, _ComposedHeaderProxy(src, alias))
            else:
                reg.register(alias, VisualSourceProxy(src, alias, verbosity))
        yield
    finally:
        # Restore original sources
        for alias, src in originals.items():
            reg.register(alias, src)
