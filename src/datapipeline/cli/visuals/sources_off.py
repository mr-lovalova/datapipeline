from contextlib import contextmanager
from typing import Iterator, Any, Optional
import logging

from datapipeline.runtime import Runtime
from datapipeline.sources.models.source import Source

from .labels import progress_meta_for_loader
from .common import log_transport_details, log_combined_stream

logger = logging.getLogger(__name__)


class _OffSourceProxy(Source):
    def __init__(self, inner: Source, alias: str):
        self._inner = inner
        self._alias = alias

    def stream(self) -> Iterator[Any]:
        loader = getattr(self._inner, "loader", None)
        transport = getattr(loader, "transport", None)
        _, unit = progress_meta_for_loader(loader)
        emitted = 0
        started = False
        try:
            for item in self._inner.stream():
                if not started:
                    try:
                        log_transport_details(transport, self._alias)
                    except Exception:
                        pass
                    started = True
                emitted += 1
                yield item
        finally:
            if logger.isEnabledFor(logging.INFO):
                unit_label = f" {unit}" if unit else ""
                logger.info("[%s] Stream complete (%d%s) ✔", self._alias, emitted, unit_label)


@contextmanager
def visual_sources(runtime: Runtime, log_level: int | None, progress_style: str = "auto"):
    if log_level is None or log_level > logging.INFO:
        yield
        return

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())

    try:
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
                        detail_entries = [str(item) for item in inputs]
                except Exception:
                    detail_entries = None
                log_combined_stream(self._alias, detail_entries)
                yield from self._inner.stream()

        for alias, src in originals.items():
            if getattr(src, "loader", None) is None:
                reg.register(alias, _ComposedHeaderProxy(src, alias))
            else:
                reg.register(alias, _OffSourceProxy(src, alias))
        yield
    finally:
        for alias, src in originals.items():
            reg.register(alias, src)
