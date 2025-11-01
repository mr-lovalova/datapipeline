from typing import Iterator, Any, Optional
from contextlib import contextmanager
from itertools import cycle
import threading
import time
from .labels import progress_meta_for_loader
from datapipeline.runtime import Runtime
from datapipeline.sources.models.source import Source
from tqdm import tqdm


class VisualSourceProxy(Source):
    """Proxy wrapping Source.stream() with CLI feedback controlled by verbosity."""

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
        bar.set_description_str(label)
        bar.refresh()

        stop_event = threading.Event()

        def _spin():
            frames = cycle((" |", " /", " -", " \\"))
            while not stop_event.is_set():
                bar.set_description_str(f"{label}{next(frames)}")
                bar.refresh()
                time.sleep(0.1)
            bar.set_description_str(label)
            bar.refresh()

        worker = threading.Thread(target=_spin, daemon=True)
        worker.start()
        return stop_event, worker, bar

    def _count_with_indicator(self, label: str) -> Optional[int]:
        try:
            stop_event, worker, bar = self._start_spinner(label)
        except Exception:
            # If spinner setup fails, silently fall back to raw count
            return self._safe_count()

        try:
            return self._safe_count()
        finally:
            stop_event.set()
            worker.join()
            bar.close()

    def _safe_count(self) -> Optional[int]:
        try:
            return self._inner.count()
        except Exception:
            return None

    def stream(self) -> Iterator[Any]:
        desc, unit = progress_meta_for_loader(self._inner.loader)
        progress_desc = f"{desc} [{self._alias}]"
        label = f"Preparing data stream for [{self._alias}]"

        if self._verbosity >= 2:
            total = self._count_with_indicator(label)
            yield from tqdm(
                self._inner.stream(),
                total=total,
                desc=progress_desc,
                unit=unit,
                dynamic_ncols=True,
                mininterval=0.0,
                miniters=1,
                leave=True,
            )
            return

        try:
            stop_event, worker, bar = self._start_spinner(progress_desc)
        except Exception:
            # Spinner isn't critical; fall back to raw stream
            yield from self._inner.stream()
            return

        try:
            for item in self._inner.stream():
                yield item
        finally:
            stop_event.set()
            worker.join()
            bar.close()


@contextmanager
def visual_sources(runtime: Runtime, verbosity: int):
    """Temporarily wrap stream sources with verbosity-aware feedback."""
    if verbosity is None or verbosity <= 0:
        yield
        return

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())
    try:
        for alias, src in originals.items():
            reg.register(alias, VisualSourceProxy(src, alias, verbosity))
        yield
    finally:
        # Restore original sources
        for alias, src in originals.items():
            reg.register(alias, src)
