import json

from datapipeline.io.protocols import HeaderCapable, Writer
from datapipeline.io.sinks import StdoutTextSink, AtomicTextFileSink


class LineWriter(Writer):
    """Text line writer (uses a text sink + serializer)."""

    def __init__(self, sink: StdoutTextSink | AtomicTextFileSink, serializer):
        self.sink = sink
        self.serializer = serializer

    def write(self, item) -> None:
        self.sink.write_text(self.serializer(item))

    def close(self) -> None:
        self.sink.close()

    def abort(self) -> None:
        abort = getattr(self.sink, "abort", None)
        if callable(abort):
            abort()
        else:
            self.close()


class HeaderJsonlMixin(HeaderCapable):
    """Provide a header write by emitting one JSON line."""

    def write_header(self, header: dict) -> None:
        self.sink.write_text(
            json.dumps({"__checkpoint__": header}, ensure_ascii=False) + "\n"
        )
