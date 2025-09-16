from __future__ import annotations

from typing import Any, Dict

from datapipeline.sources.composed_loader import ComposedRawLoader
from datapipeline.sources.transports import FsFileSource, FsGlobSource, UrlSource
from datapipeline.sources.decoders import CsvDecoder, JsonDecoder, JsonLinesDecoder


def build_loader(*, transport: str, format: str | None = None, **kwargs: Any) -> ComposedRawLoader:
    """Factory entrypoint that composes a transport and a decoder.

    Args (by transport/format):
      transport: "fs" | "url"
      format: "csv" | "json" | "json-lines" (required for fs/url)
      fs: path (str), glob (bool, optional), encoding (str, default utf-8), delimiter (csv only)
      url: url (str), headers (dict, optional), encoding (str, default utf-8)
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
        source = FsGlobSource(path, encoding=encoding) if use_glob else FsFileSource(path, encoding=encoding)
    elif t == "url":
        url = kwargs.get("url")
        if not url:
            raise ValueError("url transport requires 'url'")
        headers: Dict[str, str] = dict(kwargs.get("headers") or {})
        encoding = kwargs.get("encoding", "utf-8")
        source = UrlSource(url, headers=headers, encoding=encoding)
    else:
        raise ValueError(f"unsupported transport: {transport}")

    # Build decoder
    if fmt == "csv":
        delimiter = kwargs.get("delimiter", ";")
        decoder = CsvDecoder(delimiter=delimiter)
    elif fmt == "json":
        decoder = JsonDecoder()
    elif fmt == "json-lines":
        decoder = JsonLinesDecoder()
    else:
        raise ValueError(f"unsupported format for composed loader: {format}")

    allow_net = bool(kwargs.get("count_by_fetch", False))
    return ComposedRawLoader(source, decoder, allow_network_count=allow_net)
