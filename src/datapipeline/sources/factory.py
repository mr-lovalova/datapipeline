import glob
from pathlib import Path
from typing import Any, Mapping

from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport
from datapipeline.services.path_policy import resolve_relative_fs_loader_path
from datapipeline.sources.decoders import (
    CsvDecoder,
    Decoder,
    JsonDecoder,
    JsonLinesDecoder,
    PickleDecoder,
)
from datapipeline.sources.ports import SourceTransport


DEFAULT_ENCODING = "utf-8"
DEFAULT_CSV_DELIMITER = ";"
TRANSPORT_FS = "fs"
TRANSPORT_HTTP = "http"
FORMAT_CSV = "csv"
FORMAT_JSON = "json"
FORMAT_JSONL = "jsonl"
FORMAT_PICKLE = "pickle"


def resolve_loader_paths(
    loader_args: dict[str, Any],
    project_yaml: Path,
) -> dict[str, Any]:
    transport = loader_args.get("transport")
    if not isinstance(transport, str) or transport.lower() != TRANSPORT_FS:
        return loader_args
    raw_path = loader_args.get("path")
    if not isinstance(raw_path, str) or not raw_path or Path(raw_path).is_absolute():
        return loader_args
    resolved = dict(loader_args)
    resolved["path"] = resolve_relative_fs_loader_path(
        raw_path,
        project_yaml.parent.resolve(),
    )
    return resolved


def build_loader(
    transport: str,
    format: str | None = None,
    **kwargs: Any,
) -> DataLoader:
    """Factory entrypoint that composes a transport and a decoder.

    Args (by transport/data format):
      transport: "fs" | "http"
      format: "csv" | "json" | "jsonl" | "pickle" (required for fs/http)
      fs: path (str; glob characters select matching files), encoding (str,
          default utf-8), delimiter (csv only)
      http: url (str), headers (dict, optional), params (dict, optional), encoding (str, default utf-8), timeout_seconds (float, optional)
      csv: error_prefixes (list[str], optional)
      json: array_field (str, optional)
    """
    transport_name = _normalize_name(transport)
    data_format = _normalize_name(format)
    source = build_transport(transport_name, kwargs)
    decoder = build_decoder(data_format, transport=transport_name, args=kwargs)
    allow_net = bool(kwargs.get("count_by_fetch", False))
    return DataLoader(source, decoder, allow_network_count=allow_net)


def build_transport(
    transport: str,
    args: Mapping[str, Any],
) -> SourceTransport:
    if transport == TRANSPORT_FS:
        path = args.get("path")
        if not path:
            raise ValueError("fs transport requires 'path'")
        if "glob" in args:
            raise ValueError(
                "fs transport no longer accepts 'glob'; remove it because wildcard "
                "characters in 'path' are detected automatically"
            )
        path = str(path)
        return FsGlobTransport(path) if glob.has_magic(path) else FsFileTransport(path)
    if transport == TRANSPORT_HTTP:
        url = args.get("url")
        if not url:
            raise ValueError("http transport requires 'url'")
        return HttpTransport(
            url,
            headers=dict(args.get("headers") or {}),
            params=dict(args.get("params") or {}),
            timeout_seconds=args.get("timeout_seconds"),
        )
    raise ValueError(f"unsupported transport: {transport}")


def build_decoder(
    data_format: str,
    transport: str,
    args: Mapping[str, Any],
) -> Decoder:
    encoding = args.get("encoding", DEFAULT_ENCODING)
    if data_format == FORMAT_CSV:
        return CsvDecoder(
            delimiter=args.get("delimiter", DEFAULT_CSV_DELIMITER),
            encoding=encoding,
            error_prefixes=args.get("error_prefixes"),
        )
    if data_format == FORMAT_JSON:
        return JsonDecoder(encoding=encoding, array_field=args.get("array_field"))
    if data_format == FORMAT_JSONL:
        return JsonLinesDecoder(encoding=encoding)
    if data_format == FORMAT_PICKLE:
        if transport != TRANSPORT_FS:
            raise ValueError("pickle loader currently supported only for fs transport")
        return PickleDecoder()
    raise ValueError(f"unsupported format for IO loader: {data_format}")


def _normalize_name(value: str | None) -> str:
    return (value or "").lower()
