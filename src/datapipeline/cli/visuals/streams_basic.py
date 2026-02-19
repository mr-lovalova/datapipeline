from contextlib import contextmanager
from typing import Iterator, Any
import logging

from datapipeline.runtime import Runtime
from datapipeline.sources.models.source import Source

from .source_observability import SourceObservabilityAdapter

logger = logging.getLogger(__name__)


class VisualSourceProxy(Source):
    """Proxy wrapping Source.stream() with plain logging visuals."""

    def __init__(self, stream_source: Source, stream_id: str):
        self._inner = stream_source
        self._stream_id = stream_id

    def stream(self) -> Iterator[Any]:
        adapter = SourceObservabilityAdapter(self._inner, self._stream_id)
        emitted = 0
        started = False
        try:
            for item in self._inner.stream():
                if not started:
                    indent = adapter.current_indent()
                    adapter.log_composed_details()
                    info_lines = adapter.info_lines()
                    debug_lines = adapter.debug_lines()
                    if logger.isEnabledFor(logging.INFO):
                        if info_lines:
                            for line in info_lines:
                                logger.info("%s[%s] %s", indent, self._stream_id, line)
                        else:
                            logger.info("%s[%s] Stream starting", indent, self._stream_id)
                    if logger.isEnabledFor(logging.DEBUG):
                        for line in debug_lines:
                            logger.debug("%s[%s] %s", indent, self._stream_id, line)
                    started = True
                emitted += 1
                yield item
        finally:
            if logger.isEnabledFor(logging.INFO):
                indent = adapter.current_indent()
                unit_label = f" {adapter.unit}" if adapter.unit else ""
                logger.info("%s[%s] Stream complete (%d%s) ✔", indent, self._stream_id, emitted, unit_label)


@contextmanager
def visual_sources(runtime: Runtime, log_level: int | None):
    """Temporarily wrap stream sources with plain logging visuals."""
    level = log_level if log_level is not None else logging.INFO
    if level > logging.INFO:
        yield
        return

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())
    try:
        for stream_id, stream_source in originals.items():
            reg.register(stream_id, VisualSourceProxy(stream_source, stream_id))
        yield
    finally:
        for stream_id, stream_source in originals.items():
            reg.register(stream_id, stream_source)
