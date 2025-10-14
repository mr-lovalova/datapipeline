from typing import Iterator, Any
from contextlib import contextmanager
from datapipeline.cli.visuals import progress_meta_for_loader
from datapipeline.registries.registries import stream_sources
from datapipeline.sources.models.source import Source
from tqdm import tqdm


class VisualSourceProxy(Source):
    """Proxy wrapping Source.stream() with a tqdm progress bar (CLI-only)."""

    def __init__(self, inner: Source):
        self._inner = inner

    def stream(self) -> Iterator[Any]:
        total = self._inner.count()
        desc, unit = progress_meta_for_loader(self._inner.loader)
        return tqdm(self._inner.stream(), total=total, desc=desc, unit=unit, dynamic_ncols=True, mininterval=0.0, miniters=1, leave=True)


@contextmanager
def visual_sources():
    """Temporarily wrap all registered stream sources with VisualSourceProxy."""
    originals = dict(stream_sources.items())
    try:
        for alias, src in originals.items():
            stream_sources.register(alias, VisualSourceProxy(src))
        yield
    finally:
        # Restore original sources
        for alias, src in originals.items():
            stream_sources.register(alias, src)
