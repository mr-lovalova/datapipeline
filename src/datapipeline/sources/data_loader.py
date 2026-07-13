from typing import Iterator, Any, Optional
from .models.loader import BaseDataLoader
from .ports import SourceTransport
from .adapters.http import HttpTransport
from .decoders import Decoder


class DataLoader(BaseDataLoader):
    """Compose a SourceTransport with a row Decoder."""

    def __init__(self, transport: SourceTransport, decoder: Decoder, allow_network_count: bool = False):
        self.transport = transport
        self.decoder = decoder
        self._allow_net_count = bool(allow_network_count)
        self._current_resource_uri: str | None = None

    @property
    def current_resource_uri(self) -> str | None:
        return self._current_resource_uri

    def load(self) -> Iterator[Any]:
        try:
            for resource in self.transport.resources():
                self._current_resource_uri = resource.uri
                for row in self.decoder.decode(resource.stream):
                    yield row
        finally:
            self._current_resource_uri = None

    def count(self) -> Optional[int]:
        # Delegate counting to the decoder using the transport streams.
        # Avoid counting over network unless explicitly enabled.
        try:
            if isinstance(self.transport, HttpTransport) and not self._allow_net_count:
                return None
            total = 0
            any_stream = False
            for resource in self.transport.resources():
                any_stream = True
                c = self.decoder.count(resource.stream)
                if c is None:
                    return None
                total += int(c)
            return total if any_stream else 0
        except Exception:
            return None
