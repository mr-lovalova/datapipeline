from __future__ import annotations

from typing import Iterator, Any, Optional
from .models.loader import BaseDataLoader
from .transports import Transport, HttpTransport
from .decoders import Decoder


class DataLoader(BaseDataLoader):
    """Compose a Transport with a row Decoder."""

    def __init__(self, transport: Transport, decoder: Decoder, *, allow_network_count: bool = False):
        self.transport = transport
        self.decoder = decoder
        self._allow_net_count = bool(allow_network_count)

    def load(self) -> Iterator[Any]:
        for stream in self.transport.streams():
            for row in self.decoder.decode(stream):
                yield row

    def count(self) -> Optional[int]:
        # Delegate counting to the decoder using the transport streams.
        # Avoid counting over network unless explicitly enabled.
        try:
            if isinstance(self.transport, HttpTransport) and not self._allow_net_count:
                return None
            total = 0
            any_stream = False
            for stream in self.transport.streams():
                any_stream = True
                c = self.decoder.count(stream)
                if c is None:
                    return None
                total += int(c)
            return total if any_stream else 0
        except Exception:
            return None
