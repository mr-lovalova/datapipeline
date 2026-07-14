from collections.abc import Callable

from datapipeline.io.sinks import StdoutTextSink, AtomicTextFileSink


class LineWriter:
    """Text line writer (uses a text sink + serializer)."""

    def __init__(
        self,
        sink: StdoutTextSink | AtomicTextFileSink,
        serializer: Callable[[object], str],
    ) -> None:
        self.sink = sink
        self.serializer = serializer

    def write(self, item: object) -> None:
        self.sink.write_text(self.serializer(item))

    def close(self) -> None:
        self.sink.close()

    def abort(self) -> None:
        self.sink.abort()
