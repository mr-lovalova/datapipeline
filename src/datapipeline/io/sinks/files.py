import codecs
import gzip
import os
import stat
import tempfile
from pathlib import Path


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


def _temporary_file(dest: Path) -> tuple[int, Path]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(dir=dest.parent)
    temp = Path(temp_path)
    try:
        mode = stat.S_IMODE(dest.stat().st_mode)
        os.fchmod(fd, mode)
    except FileNotFoundError:
        pass
    except BaseException:
        os.close(fd)
        temp.unlink(missing_ok=True)
        raise
    return fd, temp


class AtomicTextFileSink:
    def __init__(
        self,
        dest: Path,
        encoding: str = "utf-8",
        overwrite: bool = True,
    ):
        self._dest = dest
        self._overwrite = overwrite
        codecs.lookup(encoding)
        fd, self._tmp = _temporary_file(dest)
        self._fh = os.fdopen(fd, "w", encoding=encoding)

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


class AtomicBinaryFileSink:
    def __init__(self, dest: Path, overwrite: bool = True):
        self._dest = dest
        self._overwrite = overwrite
        fd, self._tmp = _temporary_file(dest)
        self._fh = os.fdopen(fd, "wb")

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


class GzipBinarySink:
    def __init__(self, dest: Path, overwrite: bool = True):
        self._dest = dest
        self._overwrite = overwrite
        fd, self._tmp = _temporary_file(dest)
        self._raw = os.fdopen(fd, "wb")
        self._fh = gzip.GzipFile(
            filename="",
            fileobj=self._raw,
            mode="wb",
            mtime=0,
        )

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
