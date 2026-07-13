from pathlib import Path
import os
import tempfile
import gzip

from .base import BaseSink


def _commit_temp_file(temp: Path, dest: Path, overwrite: bool) -> None:
    if overwrite:
        os.replace(temp, dest)
        return
    try:
        os.link(temp, dest)
    except FileExistsError as exc:
        raise FileExistsError(f"{dest} already exists") from exc
    finally:
        temp.unlink(missing_ok=True)


class AtomicTextFileSink(BaseSink):
    def __init__(
        self,
        dest: Path,
        encoding: str = "utf-8",
        overwrite: bool = True,
    ):
        self._dest = dest
        self._overwrite = overwrite
        dest.parent.mkdir(parents=True, exist_ok=True)
        self._tmp = Path(
            tempfile.NamedTemporaryFile(dir=str(dest.parent), delete=False).name
        )
        self._fh = open(self._tmp, "w", encoding=encoding)

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
        _commit_temp_file(self._tmp, self._dest, self._overwrite)

    def abort(self) -> None:
        self._fh.close()
        self._tmp.unlink(missing_ok=True)


class AtomicBinaryFileSink(BaseSink):
    def __init__(self, dest: Path, overwrite: bool = True):
        self._dest = dest
        self._overwrite = overwrite
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
        _commit_temp_file(self._tmp, self._dest, self._overwrite)

    def abort(self) -> None:
        self._fh.close()
        self._tmp.unlink(missing_ok=True)


class GzipBinarySink(BaseSink):
    def __init__(self, dest: Path, overwrite: bool = True):
        self._dest = dest
        self._overwrite = overwrite
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
        _commit_temp_file(self._tmp, self._dest, self._overwrite)

    def abort(self) -> None:
        self._fh.close()
        self._raw.close()
        self._tmp.unlink(missing_ok=True)
