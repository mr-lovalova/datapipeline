from contextlib import contextmanager
from typing import Iterator, Any
import logging

from datapipeline.runtime import Runtime
from datapipeline.sources.models.source import Source

from .source_observability import SourceObservabilityAdapter
from .execution_context import (
    reset_current_source_visual_proxy_factory,
    reset_current_visual_log_level,
    set_current_source_visual_proxy_factory,
    set_current_visual_log_level,
)

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

        def _emit_source_details() -> None:
            info_indent = adapter.current_indent(logging.INFO)
            debug_indent = adapter.current_indent(logging.DEBUG)
            info_lines = adapter.info_lines()
            debug_lines = adapter.debug_lines()
            if logger.isEnabledFor(logging.INFO):
                if info_lines:
                    for line in info_lines:
                        logger.info("%s[%s] %s", info_indent, self._stream_id, line)
                else:
                    logger.info("%s[%s] Stream starting", info_indent, self._stream_id)
            if logger.isEnabledFor(logging.DEBUG):
                for line in debug_lines:
                    logger.debug("%s[%s] %s", debug_indent, self._stream_id, line)

        try:
            if not getattr(adapter, "progress_visible", lambda: True)():
                _emit_source_details()
                started = True
            for item in self._inner.stream():
                if not started:
                    _emit_source_details()
                    started = True
                emitted += 1
                yield item
        finally:
            if logger.isEnabledFor(logging.INFO):
                indent = adapter.current_indent(logging.INFO)
                logger.info(
                    "%s[%s] Stream complete items=%d",
                    indent,
                    self._stream_id,
                    emitted,
                )


@contextmanager
def visual_sources(runtime: Runtime, log_level: int | None):
    """Temporarily wrap stream sources with plain logging visuals."""
    level = log_level if log_level is not None else logging.INFO

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())
    level_token = set_current_visual_log_level(level)
    proxy_token = set_current_source_visual_proxy_factory(
        lambda stream_source, stream_id: VisualSourceProxy(stream_source, stream_id)
    )
    try:
        for stream_id, stream_source in originals.items():
            reg.register(stream_id, VisualSourceProxy(stream_source, stream_id))
        yield
    finally:
        for stream_id, stream_source in originals.items():
            reg.register(stream_id, stream_source)
        reset_current_source_visual_proxy_factory(proxy_token)
        reset_current_visual_log_level(level_token)
