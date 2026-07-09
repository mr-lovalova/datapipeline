from typing import Iterator, List

from datapipeline.sources.ports import SourceResource, SourceTransport


class FsFileTransport(SourceTransport):
    def __init__(self, path: str, chunk_size: int = 65536):
        self.path = path
        self.chunk_size = chunk_size

    def resources(self) -> Iterator[SourceResource]:
        def _iter() -> Iterator[bytes]:
            with open(self.path, "rb") as f:
                while True:
                    chunk = f.read(self.chunk_size)
                    if not chunk:
                        break
                    yield chunk

        yield SourceResource(uri=self.path, stream=_iter())


class FsGlobTransport(SourceTransport):
    def __init__(self, pattern: str, chunk_size: int = 65536):
        import glob as _glob

        self.pattern = pattern
        self.chunk_size = chunk_size
        self._files: List[str] = sorted(_glob.glob(pattern))

    @property
    def files(self) -> List[str]:
        return list(self._files)

    def resources(self) -> Iterator[SourceResource]:
        def _iter(path: str) -> Iterator[bytes]:
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(self.chunk_size)
                    if not chunk:
                        break
                    yield chunk

        for p in self._files:
            yield SourceResource(uri=p, stream=_iter(p))
