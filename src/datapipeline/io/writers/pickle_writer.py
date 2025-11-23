import pickle
from pathlib import Path
from typing import Optional

from datapipeline.io.serializers import pickle_serializer, BasePickleSerializer
from datapipeline.io.protocols import HasFilePath, Writer
from datapipeline.io.sinks import AtomicBinaryFileSink


class PickleFileWriter(Writer, HasFilePath):
    def __init__(
        self,
        dest: Path,
        serializer: BasePickleSerializer | None = None,
        protocol: int = pickle.HIGHEST_PROTOCOL,
    ):
        self.sink = AtomicBinaryFileSink(dest)
        self.pickler = pickle.Pickler(self.sink.fh, protocol=protocol)
        self._serializer = serializer or pickle_serializer("sample")

    @property
    def file_path(self) -> Optional[Path]:
        return self.sink.file_path

    def write(self, item) -> None:
        self.pickler.dump(self._serializer(item))
        self.pickler.clear_memo()

    def close(self) -> None:
        self.sink.close()
