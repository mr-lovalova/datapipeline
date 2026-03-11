from typing import Iterable, Iterator, List, Optional

from datapipeline.sources.ports import SourceTransport


class FsFileTransport(SourceTransport):
    def __init__(self, path: str, chunk_size: int = 65536):
        self.path = path
        self.chunk_size = chunk_size

    def streams(self) -> Iterator[Iterable[bytes]]:
        def _iter() -> Iterator[bytes]:
            with open(self.path, "rb") as f:
                while True:
                    chunk = f.read(self.chunk_size)
                    if not chunk:
                        break
                    yield chunk

        yield _iter()


class FsGlobTransport(SourceTransport):
    def __init__(self, pattern: str, chunk_size: int = 65536):
        import glob as _glob

        self.pattern = pattern
        self.chunk_size = chunk_size
        self._files: List[str] = sorted(_glob.glob(pattern))
        self._current_path: Optional[str] = None

    @property
    def files(self) -> List[str]:
        return list(self._files)

    @property
    def current_path(self) -> Optional[str]:
        return self._current_path

    def streams(self) -> Iterator[Iterable[bytes]]:
        def _iter(path: str) -> Iterator[bytes]:
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(self.chunk_size)
                    if not chunk:
                        break
                    yield chunk

        try:
            for p in self._files:
                self._current_path = p
                yield _iter(p)
        finally:
            self._current_path = None
