from contextlib import contextmanager
from typing import Iterator, Any
import logging

from datapipeline.runtime import Runtime
from datapipeline.sources.models.source import Source

from .source_observability import SourceObservabilityAdapter

logger = logging.getLogger(__name__)


class _OffSourceProxy(Source):
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
                    if logger.isEnabledFor(logging.INFO):
                        for line in adapter.info_lines():
                            logger.info("%s[%s] %s", indent, self._stream_id, line)
                    if logger.isEnabledFor(logging.DEBUG):
                        for line in adapter.debug_lines():
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
def visual_sources(runtime: Runtime, log_level: int | None, progress_style: str = "auto"):
    if log_level is None or log_level > logging.INFO:
        yield
        return

    reg = runtime.registries.stream_sources
    originals = dict(reg.items())

    try:
        for stream_id, stream_source in originals.items():
            reg.register(stream_id, _OffSourceProxy(stream_source, stream_id))
        yield
    finally:
        for stream_id, stream_source in originals.items():
            reg.register(stream_id, stream_source)
