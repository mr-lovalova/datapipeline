from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Iterator, List, Dict, Optional, Any
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse


class Transport(ABC):
    """Abstract transport that yields raw-byte streams per resource."""

    @abstractmethod
    def streams(self) -> Iterator[Iterable[bytes]]:
        pass


class FsFileTransport(Transport):
    def __init__(self, path: str, *, chunk_size: int = 65536):
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


class FsGlobTransport(Transport):
    def __init__(self, pattern: str, *, chunk_size: int = 65536):
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


class HttpTransport(Transport):
    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None, params: Optional[Dict[str, Any]] = None, chunk_size: int = 64 * 1024):
        self.url = url
        self.headers = dict(headers or {})
        self.params: Dict[str, Any] = dict(params or {})
        self.chunk_size = chunk_size

    def _build_url(self) -> str:
        if not self.params:
            return self.url
        try:
            parsed = urlparse(self.url)
            existing = parse_qsl(parsed.query, keep_blank_values=True)
            merged = existing + list(self.params.items())
            query = urlencode(merged, doseq=True)
            return urlunparse(parsed._replace(query=query))
        except Exception:
            return self.url

    def streams(self) -> Iterator[Iterable[bytes]]:
        req_url = self._build_url()
        req = Request(req_url, headers=self.headers)

        try:
            resp = urlopen(req)
        except (URLError, HTTPError) as e:
            raise RuntimeError(f"failed to fetch {self.url}: {e}") from e

        def byte_stream() -> Iterator[bytes]:
            with resp:
                while True:
                    chunk = resp.read(self.chunk_size)
                    if not chunk:
                        break
                    yield chunk

        yield byte_stream()
