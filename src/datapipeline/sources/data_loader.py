from collections.abc import Iterator
from typing import Any

from .models.loader import BaseDataLoader, SourceProgressUnit
from .ports import SourceTransport
from .decoders import (
    CsvDecoder,
    Decoder,
    JsonDecoder,
    JsonLinesDecoder,
)


class DataLoader(BaseDataLoader):
    """Compose a SourceTransport with a row Decoder."""

    def __init__(
        self,
        transport: SourceTransport,
        decoder: Decoder,
    ):
        self.transport = transport
        self.decoder = decoder
        self._current_resource_uri: str | None = None

    @property
    def current_resource_uri(self) -> str | None:
        return self._current_resource_uri

    @property
    def progress_unit(self) -> SourceProgressUnit:
        if isinstance(self.decoder, CsvDecoder):
            return "rows"
        if isinstance(self.decoder, (JsonDecoder, JsonLinesDecoder)):
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
