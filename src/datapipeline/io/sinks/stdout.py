import sys
from typing import Optional

from .base import BaseSink


class StdoutTextSink(BaseSink):
    def __init__(self, stream: Optional[object] = None):
        self.stream = stream or sys.stdout

    def write_text(self, s: str) -> None:
        self.stream.write(s)

    def flush(self) -> None:
        self.stream.flush()

    def close(self) -> None:
        self.flush()
