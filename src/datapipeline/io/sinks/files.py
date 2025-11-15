from pathlib import Path
import os
import tempfile
import gzip

from .base import BaseSink


class AtomicTextFileSink(BaseSink):
    def __init__(self, dest: Path):
        self._dest = dest
        dest.parent.mkdir(parents=True, exist_ok=True)
        self._tmp = Path(
            tempfile.NamedTemporaryFile(dir=str(dest.parent), delete=False).name
        )
        self._fh = open(self._tmp, "w", encoding="utf-8")

    @property
    def file_path(self) -> Path:
        return self._dest

    @property
    def fh(self):
        return self._fh

    def write_text(self, s: str) -> None:
        self._fh.write(s)

    def close(self) -> None:
        self._fh.close()
        os.replace(self._tmp, self._dest)


class AtomicBinaryFileSink(BaseSink):
    def __init__(self, dest: Path):
        self._dest = dest
        dest.parent.mkdir(parents=True, exist_ok=True)
        self._tmp = Path(
            tempfile.NamedTemporaryFile(dir=str(dest.parent), delete=False).name
        )
        self._fh = open(self._tmp, "wb")

    @property
    def file_path(self) -> Path:
        return self._dest

    @property
    def fh(self):
        return self._fh

    def write_bytes(self, b: bytes) -> None:
        self._fh.write(b)

    def close(self) -> None:
        self._fh.close()
        os.replace(self._tmp, self._dest)


class GzipBinarySink(BaseSink):
    def __init__(self, dest: Path):
        self._dest = dest
        dest.parent.mkdir(parents=True, exist_ok=True)
        self._tmp = Path(
            tempfile.NamedTemporaryFile(dir=str(dest.parent), delete=False).name
        )
        self._raw = open(self._tmp, "wb")
        self._fh = gzip.GzipFile(fileobj=self._raw, mode="wb")

    @property
    def file_path(self) -> Path:
        return self._dest

    def write_bytes(self, b: bytes) -> None:
        self._fh.write(b)

    def close(self) -> None:
        self._fh.close()
        self._raw.close()
        os.replace(self._tmp, self._dest)
