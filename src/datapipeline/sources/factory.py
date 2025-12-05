from __future__ import annotations

from typing import Any, Dict

from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.transports import FsFileTransport, FsGlobTransport, HttpTransport
from datapipeline.sources.decoders import (
    CsvDecoder,
    JsonDecoder,
    JsonLinesDecoder,
    PickleDecoder,
)


def build_loader(*, transport: str, format: str | None = None, **kwargs: Any) -> DataLoader:
    """Factory entrypoint that composes a transport and a decoder.

    Args (by transport/format):
      transport: "fs" | "http"
      format: "csv" | "json" | "json-lines" | "pickle" (required for fs/http)
      fs: path (str), glob (bool, optional), encoding (str, default utf-8), delimiter (csv only)
      http: url (str), headers (dict, optional), params (dict, optional), encoding (str, default utf-8)
    """

    t = (transport or "").lower()
    fmt = (format or "").lower()

    # Build source
    if t == "fs":
        path = kwargs.get("path")
        if not path:
            raise ValueError("fs transport requires 'path'")
        encoding = kwargs.get("encoding", "utf-8")
        use_glob = bool(kwargs.get("glob", False))
        source = FsGlobTransport(path) if use_glob else FsFileTransport(path)
    elif t == "http":
        url = kwargs.get("url")
        if not url:
            raise ValueError("http transport requires 'url'")
        headers: Dict[str, str] = dict(kwargs.get("headers") or {})
        params: Dict[str, Any] = dict(kwargs.get("params") or {})
        encoding = kwargs.get("encoding", "utf-8")
        source = HttpTransport(url, headers=headers, params=params)
    else:
        raise ValueError(f"unsupported transport: {transport}")

    # Build decoder
    if fmt == "csv":
        delimiter = kwargs.get("delimiter", ";")
        decoder = CsvDecoder(delimiter=delimiter, encoding=encoding)
    elif fmt == "json":
        decoder = JsonDecoder(encoding=encoding)
    elif fmt == "json-lines":
        decoder = JsonLinesDecoder(encoding=encoding)
    elif fmt == "pickle":
        if t != "fs":
            raise ValueError("pickle loader currently supported only for fs transport")
        decoder = PickleDecoder()
    else:
        raise ValueError(f"unsupported format for IO loader: {format}")

    allow_net = bool(kwargs.get("count_by_fetch", False))
    return DataLoader(source, decoder, allow_network_count=allow_net)
