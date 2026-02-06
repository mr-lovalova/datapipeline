from __future__ import annotations

import pickle
import tempfile
import weakref
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Any


_LEN_BYTES = 8


def _encode_len(size: int) -> bytes:
    return int(size).to_bytes(_LEN_BYTES, "little", signed=False)


def _decode_len(raw: bytes) -> int:
    return int.from_bytes(raw, "little", signed=False)


@dataclass
class _SpoolState:
    writer: Any
    path: Path
    offsets: list[int]
    source: Iterator[Any]
    done: bool = False

    def close(self) -> None:
        try:
            self.writer.close()
        except Exception:
            pass


class SpoolCache:
    """Disk-backed cache for iterators with multiple sequential readers."""

    def __init__(self, source: Iterator[Any], *, name: str | None = None) -> None:
        tmp = tempfile.NamedTemporaryFile(
            prefix=f"dp-spool-{name or 'stream'}-",
            suffix=".pkl",
            delete=False,
        )
        path = Path(tmp.name)
        self._state = _SpoolState(
            writer=tmp,
            path=path,
            offsets=[],
            source=iter(source),
        )
        self._finalizer = weakref.finalize(self, _cleanup, path, tmp)

    @property
    def path(self) -> Path:
        return self._state.path

    def close(self) -> None:
        """Close writer and remove the spool file."""
        if self._finalizer.alive:
            self._finalizer()

    def __enter__(self) -> "SpoolCache":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def reader(self) -> Iterator[Any]:
        return _SpoolReader(self)

    def _append_next(self) -> bool:
        if self._state.done:
            return False
        try:
            item = next(self._state.source)
        except StopIteration:
            self._state.done = True
            self._state.writer.flush()
            return False
        try:
            data = pickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as exc:  # pragma: no cover - defensive
            raise TypeError(
                "SpoolCache requires picklable records for multi-feature fanout."
            ) from exc
        offset = self._state.writer.tell()
        self._state.writer.write(_encode_len(len(data)))
        self._state.writer.write(data)
        self._state.writer.flush()
        self._state.offsets.append(offset)
        return True

    def _ensure_index(self, index: int) -> None:
        while len(self._state.offsets) <= index:
            if not self._append_next():
                break


class _SpoolReader:
    def __init__(self, cache: SpoolCache) -> None:
        self._cache = cache
        self._index = 0
        self._fh = open(cache.path, "rb")

    def __iter__(self) -> "_SpoolReader":
        return self

    def __next__(self) -> Any:
        self._cache._ensure_index(self._index)
        if self._index >= len(self._cache._state.offsets):
            self._close()
            raise StopIteration
        offset = self._cache._state.offsets[self._index]
        self._index += 1
        self._fh.seek(offset)
        raw = self._fh.read(_LEN_BYTES)
        if not raw:
            self._close()
            raise StopIteration
        size = _decode_len(raw)
        payload = self._fh.read(size)
        return pickle.loads(payload)

    def _close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass

    def __del__(self) -> None:
        self._close()


def _cleanup(path: Path, writer: Any) -> None:
    try:
        writer.close()
    except Exception:
        pass
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass
