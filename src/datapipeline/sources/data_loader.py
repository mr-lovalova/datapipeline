from collections.abc import Iterator
from typing import Any

from .models.loader import BaseDataLoader, SourceProgressUnit
from .ports import SourceTransport
from .adapters.http import HttpTransport
from .decoders import (
    CsvDecoder,
    Decoder,
    JsonDecoder,
    JsonLinesDecoder,
    PickleDecoder,
)


class DataLoader(BaseDataLoader):
    """Compose a SourceTransport with a row Decoder."""

    def __init__(
        self,
        transport: SourceTransport,
        decoder: Decoder,
        allow_network_count: bool = False,
    ):
        self.transport = transport
        self.decoder = decoder
        self._allow_net_count = bool(allow_network_count)
        self._current_resource_uri: str | None = None

    @property
    def current_resource_uri(self) -> str | None:
        return self._current_resource_uri

    @property
    def progress_unit(self) -> SourceProgressUnit:
        if isinstance(self.decoder, CsvDecoder):
            return "rows"
        if isinstance(self.decoder, (JsonDecoder, JsonLinesDecoder, PickleDecoder)):
            return "items"
        return "records"

    def load(self) -> Iterator[Any]:
        resources = iter(self.transport.resources())
        try:
            for resource in resources:
                self._current_resource_uri = resource.uri
                stream = iter(resource.stream)
                try:
                    yield from self.decoder.decode(stream)
                finally:
                    close = getattr(stream, "close", None)
                    if callable(close):
                        close()
        finally:
            close = getattr(resources, "close", None)
            if callable(close):
                close()
            self._current_resource_uri = None

    def count(self) -> int | None:
        if isinstance(self.transport, HttpTransport) and not self._allow_net_count:
            return None
        resources = iter(self.transport.resources())
        total = 0
        any_stream = False
        try:
            for resource in resources:
                any_stream = True
                stream = iter(resource.stream)
                try:
                    count = self.decoder.count(stream)
                finally:
                    close = getattr(stream, "close", None)
                    if callable(close):
                        close()
                if count is None:
                    return None
                total += count
            return total if any_stream else 0
        finally:
            close = getattr(resources, "close", None)
            if callable(close):
                close()
