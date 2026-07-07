from typing import Any, Dict, Iterable, Iterator, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from datapipeline.sources.ports import SourceResource, SourceTransport


class HttpTransport(SourceTransport):
    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 64 * 1024,
        timeout_seconds: Optional[float] = None,
    ):
        self.url = url
        self.headers = dict(headers or {})
        self.params: Dict[str, Any] = dict(params or {})
        self.chunk_size = chunk_size
        self.timeout_seconds = timeout_seconds

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

    def resources(self) -> Iterator[SourceResource]:
        req_url = self._build_url()
        req = Request(req_url, headers=self.headers)

        try:
            resp = urlopen(req, timeout=self.timeout_seconds)
        except (URLError, HTTPError) as e:
            raise RuntimeError(f"failed to fetch {self.url}: {e}") from e

        def byte_stream() -> Iterator[bytes]:
            with resp:
                while True:
                    chunk = resp.read(self.chunk_size)
                    if not chunk:
                        break
                    yield chunk

        yield SourceResource(uri=req_url, stream=byte_stream())
