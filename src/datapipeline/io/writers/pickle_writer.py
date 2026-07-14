import pickle
from pathlib import Path

from datapipeline.io.serializers import pickle_serializer
from datapipeline.io.sinks import AtomicBinaryFileSink


class PickleFileWriter:
    def __init__(
        self,
        dest: Path,
        serializer=None,
        protocol: int = pickle.HIGHEST_PROTOCOL,
        overwrite: bool = True,
    ):
        self.sink = AtomicBinaryFileSink(dest, overwrite=overwrite)
        self.pickler = pickle.Pickler(self.sink.fh, protocol=protocol)
        self._serializer = serializer or pickle_serializer()

    def write(self, item) -> None:
        self.pickler.dump(self._serializer(item))
        self.pickler.clear_memo()

    def close(self) -> None:
        self.sink.close()

    def abort(self) -> None:
        self.sink.abort()
