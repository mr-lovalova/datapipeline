from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, Iterator, List, Dict, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


class TextSource(ABC):
    """Abstract transport that yields text line streams per resource.

    Each item from `streams()` is an iterable over text lines (no newlines).
    """

    @abstractmethod
    def streams(self) -> Iterator[Iterable[str]]:
        pass


class FsFileSource(TextSource):
    def __init__(self, path: str, *, encoding: str = "utf-8"):
        self.path = path
        self.encoding = encoding

    def streams(self) -> Iterator[Iterable[str]]:
        def _iter() -> Iterator[str]:
            with open(self.path, "r", encoding=self.encoding) as f:
                for line in f:
                    yield line
        yield _iter()


class FsGlobSource(TextSource):
    def __init__(self, pattern: str, *, encoding: str = "utf-8"):
        import glob as _glob

        self.pattern = pattern
        self.encoding = encoding
        self._files: List[str] = sorted(_glob.glob(pattern))

    def streams(self) -> Iterator[Iterable[str]]:
        def _iter(path: str) -> Iterator[str]:
            with open(path, "r", encoding=self.encoding) as f:
                for line in f:
                    yield line
        for p in self._files:
            yield _iter(p)


class UrlSource(TextSource):
    def __init__(self, url: str, *, headers: Optional[Dict[str, str]] = None, encoding: str = "utf-8"):
        self.url = url
        self.headers = dict(headers or {})
        self.encoding = encoding

    def streams(self) -> Iterator[Iterable[str]]:
        req = Request(self.url, headers=self.headers)
        try:
            with urlopen(req) as resp:
                data = resp.read()
        except (URLError, HTTPError) as e:
            raise RuntimeError(f"failed to fetch {self.url}: {e}") from e

        text = data.decode(self.encoding, errors="strict")
        lines = text.splitlines()
        yield iter(lines)
