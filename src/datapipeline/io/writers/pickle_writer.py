import pickle
from pathlib import Path

from datapipeline.io.normalization import raw_payload
from datapipeline.io.sinks.files import AtomicBinaryFileSink


class PickleFileWriter:
    def __init__(self, dest: Path) -> None:
        self.sink = AtomicBinaryFileSink(dest)
        self.pickler = pickle.Pickler(self.sink.fh, protocol=pickle.HIGHEST_PROTOCOL)

    def write(self, item: object) -> None:
        self.pickler.dump(raw_payload(item))
        self.pickler.clear_memo()

    def close(self) -> None:
        self.sink.close()

    def abort(self) -> None:
        self.sink.abort()
