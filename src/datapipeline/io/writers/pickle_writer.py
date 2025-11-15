import pickle
from pathlib import Path
from typing import Optional

from datapipeline.io.formatters import PickleFormatter
from datapipeline.io.protocols import HasFilePath, Writer
from datapipeline.io.sinks import AtomicBinaryFileSink


class PickleFileWriter(Writer, HasFilePath):
    def __init__(self, dest: Path, protocol: int = pickle.HIGHEST_PROTOCOL):
        self.sink = AtomicBinaryFileSink(dest)
        self.pickler = pickle.Pickler(self.sink.fh, protocol=protocol)
        self._fmt = PickleFormatter()

    @property
    def file_path(self) -> Optional[Path]:
        return self.sink.file_path

    def write(self, item) -> None:
        self.pickler.dump(self._fmt(item))
        self.pickler.clear_memo()

    def close(self) -> None:
        self.sink.close()
