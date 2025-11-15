import json
from typing import Optional

from datapipeline.io.protocols import HeaderCapable, Writer
from datapipeline.io.sinks import StdoutTextSink, AtomicTextFileSink


class LineWriter(Writer):
    """Text line writer (uses a text sink + string formatter)."""

    def __init__(self, sink: StdoutTextSink | AtomicTextFileSink, formatter):
        self.sink = sink
        self.fmt = formatter

    def write(self, item) -> None:
        self.sink.write_text(self.fmt(item))

    def close(self) -> None:
        self.sink.close()


class HeaderJsonlMixin(HeaderCapable):
    """Provide a header write by emitting one JSON line."""

    def write_header(self, header: dict) -> None:
        self.sink.write_text(
            json.dumps({"__checkpoint__": header}, ensure_ascii=False) + "\n"
        )
