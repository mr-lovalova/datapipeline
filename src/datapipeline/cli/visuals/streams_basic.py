from contextlib import contextmanager
from typing import Iterator, Any
import logging

from datapipeline.runtime import Runtime
from datapipeline.sources.models.source import Source

from .execution import emit_source_info
from .source_observability import (
    SourceObservabilityAdapter,
    supports_source_observability,
)
from .execution_context import (
    current_source_depth,
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

    @property
    def loader(self):
        return self._inner.loader

    def stream(self) -> Iterator[Any]:
        adapter = SourceObservabilityAdapter(self._inner, self._stream_id)
        started = False

        def _emit_source_details() -> None:
            debug_indent = adapter.current_indent(logging.DEBUG)
            info_lines = adapter.info_lines()
            debug_lines = adapter.debug_lines()
            for line in info_lines:
                emit_source_info(
                    self._stream_id,
                    line,
                    logger=logger,
                    depth=current_source_depth(),
                )
            if logger.isEnabledFor(logging.DEBUG):
                for line in debug_lines:
                    logger.debug("%s[%s] %s", debug_indent, self._stream_id, line)

        if not getattr(adapter, "progress_visible", lambda: True)():
            _emit_source_details()
            started = True
        for item in self._inner.stream():
            if not started:
                _emit_source_details()
                started = True
            yield item


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
            if supports_source_observability(stream_source):
                reg.register(stream_id, VisualSourceProxy(stream_source, stream_id))
            else:
                reg.register(stream_id, stream_source)
        yield
    finally:
        for stream_id, stream_source in originals.items():
            reg.register(stream_id, stream_source)
        reset_current_source_visual_proxy_factory(proxy_token)
        reset_current_visual_log_level(level_token)
