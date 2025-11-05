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

    @staticmethod
    def _compute_glob_root(files: list[str]) -> Optional[Path]:
        if not files:
            return None
        try:
            return Path(os.path.commonpath(files))
        except Exception:
            return None

    @staticmethod
    def _compact_root(path: Path, *, segments: int = 3) -> str:
        parts = [part for part in path.as_posix().split("/") if part]
        if len(parts) > segments:
            parts = ["..."] + parts[-segments:]
        return "/".join(parts) if parts else "/"

    @staticmethod
    def _relative_label(path: str, root: Optional[Path]) -> str:
        if root is not None:
            try:
                rel = Path(path).relative_to(root)
                rel_str = rel.as_posix()
                if rel_str:
                    return rel_str
                return rel.name or path
            except Exception:
                pass
        return Path(path).name or path

    def _log_source_details(self, transport, root: Optional[Path]) -> None:
        if not logger.isEnabledFor(logging.INFO):
            return
        if not isinstance(transport, FsGlobTransport):
            return
        pattern = getattr(transport, "pattern", "")
        files = transport.files
        total = len(files)
        if total == 0:
            logger.info("fs.glob pattern %s matched no files", pattern)
            return
        root_label = self._compact_root(root) if root else pattern or "fs"
        logger.info("fs.glob matched %d file(s) under %s", total, root_label)
        rel_files = [self._relative_label(path, root) for path in files]
        if logger.isEnabledFor(logging.DEBUG):
            for idx, path in enumerate(rel_files, start=1):
                logger.debug("fs.glob file %d/%d: %s", idx, total, path)
            return
        if total == 1:
            logger.info("fs.glob streaming %s", rel_files[0])
            return
        if total <= 6:
            for idx, path in enumerate(rel_files, start=1):
                logger.info("fs.glob file %d/%d: %s", idx, total, path)
            return
        logger.info("fs.glob first file: %s", rel_files[0])
        logger.info("fs.glob last file: %s", rel_files[-1])
        logger.info(
            "fs.glob (%d intermediate file(s) omitted); switch to DEBUG for full listing",
            total - 2,
        )

    @staticmethod
    def _current_path_label(transport, root: Optional[Path]) -> Optional[str]:
        if not isinstance(transport, FsGlobTransport):
            return None
        current = transport.current_path
        if not current:
            return None
        return VisualSourceProxy._relative_label(current, root)

    def stream(self) -> Iterator[Any]:
        desc, unit = progress_meta_for_loader(self._inner.loader)
        prefix, sep, suffix = desc.partition(": ")
        header = f"{prefix}:" if sep else desc
        tail = suffix if sep else None
        label = f"[{self._alias}] Preparing data stream"

        loader = getattr(self._inner, "loader", None)
        transport = getattr(loader, "transport", None)

        glob_root: Optional[Path] = None
        if isinstance(transport, FsGlobTransport):
            glob_root = self._compute_glob_root(transport.files)

        self._log_source_details(transport, glob_root)

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
            current_label = self._current_path_label(transport, glob_root)
            if not current_label or current_label == last_path_label:
                return
            last_path_label = current_label
            apply_label(current_label)

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

            def update_progress(name: str) -> None:
                bar.set_description_str(compose_desc(name))
                bar.refresh()

            try:
                for item in self._inner.stream():
                    maybe_update_label(update_progress)
                    bar.update()
                    yield item
            finally:
                bar.close()
            return

        try:
            state, stop_event, worker, bar = self._start_spinner(compose_desc(None))
        except Exception:
            # Spinner isn't critical; fall back to raw stream
            yield from self._inner.stream()
            return

        def update_spinner(name: str) -> None:
            state["base"] = compose_desc(name)
            bar.set_description_str(state["base"])
            bar.refresh()

        try:
            for item in self._inner.stream():
                maybe_update_label(update_spinner)
                yield item
        finally:
            self._stop_spinner(stop_event, worker, bar)


@contextmanager
def visual_sources(runtime: Runtime, log_level: int):
    """Temporarily wrap stream sources with logging-level-driven feedback."""
    if log_level is None or log_level > logging.INFO:
        yield
        return

    verbosity = 2 if log_level <= logging.DEBUG else 1

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
