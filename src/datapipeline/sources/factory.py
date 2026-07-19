import glob
from collections.abc import Mapping, Sequence
from typing import Any

from datapipeline.io.compression import Compression
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport
from datapipeline.sources.decoders import (
    CsvDecoder,
    Decoder,
    JsonDecoder,
    JsonLinesDecoder,
)
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.ports import SourceTransport


DEFAULT_ENCODING = "utf-8"
DEFAULT_CSV_DELIMITER = ";"
TRANSPORT_FS = "fs"
TRANSPORT_HTTP = "http"
FORMAT_CSV = "csv"
FORMAT_JSON = "json"
FORMAT_JSONL = "jsonl"


def build_loader(
    transport: str,
    format: str,
    *,
    path: str | None = None,
    url: str | None = None,
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    encoding: str = DEFAULT_ENCODING,
    delimiter: str | None = None,
    error_prefixes: Sequence[str] | None = None,
    array_field: str | None = None,
    timeout_seconds: float | None = None,
    compression: Compression | None = None,
) -> DataLoader:
    transport = transport.lower()
    format = format.lower()

    if compression not in {None, "gzip"}:
        raise ValueError(f"unsupported compression: {compression}")
    if compression is not None and transport != TRANSPORT_FS:
        raise ValueError("compression is supported only for fs transport")
    if compression == "gzip" and format not in {FORMAT_CSV, FORMAT_JSONL}:
        raise ValueError("gzip compression is supported only for csv and jsonl formats")

    source: SourceTransport
    if transport == TRANSPORT_FS:
        if not path:
            raise ValueError("fs transport requires 'path'")
        source = (
            FsGlobTransport(path, compression=compression)
            if glob.has_magic(path)
            else FsFileTransport(path, compression=compression)
        )
    elif transport == TRANSPORT_HTTP:
        if not url:
            raise ValueError("http transport requires 'url'")
        source = HttpTransport(
            url,
            headers=headers,
            params=params,
            timeout_seconds=timeout_seconds,
        )
    else:
        raise ValueError(f"unsupported transport: {transport}")

    decoder: Decoder
    if format == FORMAT_CSV:
        decoder = CsvDecoder(
            delimiter=delimiter or DEFAULT_CSV_DELIMITER,
            encoding=encoding,
            error_prefixes=error_prefixes,
        )
    elif format == FORMAT_JSON:
        decoder = JsonDecoder(encoding=encoding, array_field=array_field)
    elif format == FORMAT_JSONL:
        decoder = JsonLinesDecoder(encoding=encoding)
    else:
        raise ValueError(f"unsupported format for IO loader: {format}")

    return DataLoader(source, decoder)
