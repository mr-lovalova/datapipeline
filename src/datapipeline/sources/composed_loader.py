from __future__ import annotations

from typing import Iterator, Any, Optional
from .models.loader import RawDataLoader
from .transports import TextSource, UrlSource
from .decoders import Decoder


class ComposedRawLoader(RawDataLoader):
    """Compose a transport TextSource with a row Decoder."""

    def __init__(self, source: TextSource, decoder: Decoder, *, allow_network_count: bool = False):
        self.source = source
        self.decoder = decoder
        self._allow_net_count = bool(allow_network_count)

    def load(self) -> Iterator[Any]:
        for stream in self.source.streams():
            for row in self.decoder.decode(stream):
                yield row

    def count(self) -> Optional[int]:
        # Delegate counting to the decoder using the transport streams.
        # Avoid counting over network unless explicitly enabled.
        try:
            if isinstance(self.source, UrlSource) and not self._allow_net_count:
                return None
            total = 0
            any_stream = False
            for stream in self.source.streams():
                any_stream = True
                c = self.decoder.count(stream)
                if c is None:
                    return None
                total += int(c)
            return total if any_stream else 0
        except Exception:
            return None
