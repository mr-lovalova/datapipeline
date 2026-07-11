import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport
from datapipeline.sources.decoders import (
    CsvDecoder,
    JsonDecoder,
    JsonLinesDecoder,
    PickleDecoder,
)
from datapipeline.sources.models.loader import SyntheticLoader


@dataclass(frozen=True)
class LoaderObservability:
    transport: Any | None
    glob_root: Path | None = None


@dataclass(frozen=True)
class SourceProgressEntry:
    source_resource_id: int | str
    label: str


def source_metadata(stream_source: Any) -> dict[str, Any] | None:
    loader = getattr(stream_source, "loader", None)
    if loader is None:
        return None
    return loader_source_metadata(loader)


def loader_source_metadata(loader: Any) -> dict[str, Any] | None:
    if isinstance(loader, ForeachLoader):
        return _foreach_source_metadata(loader)
    return transport_source_metadata(getattr(loader, "transport", None))


def transport_source_metadata(transport: Any) -> dict[str, Any] | None:
    if isinstance(transport, FsFileTransport):
        path = getattr(transport, "path", "")
        return {"transport": "fs.file", "file": Path(path).name or str(path)}

    if isinstance(transport, FsGlobTransport):
        files = transport.files
        total = len(files)
        metadata: dict[str, Any] = {"transport": "fs.glob", "count": total}
        root = _glob_root(files)
        if total == 0:
            pattern = getattr(transport, "pattern", "")
            metadata["root"] = _compact_root(root) if root else pattern or "fs"
        elif total == 1:
            metadata["file"] = _relative_label(files[0], root)
        else:
            metadata["first"] = _relative_label(files[0], root)
            metadata["last"] = _relative_label(files[-1], root)
        return metadata

    if isinstance(transport, HttpTransport):
        url = getattr(transport, "url", "")
        parts = urlparse(url)
        host = parts.netloc or "http"
        resource = Path(parts.path or "").name
        metadata = {"transport": "http.fetch", "host": host}
        if resource:
            metadata["resource"] = resource
        return metadata

    return None


def unit_for_loader(loader: Any) -> str:
    if isinstance(loader, SyntheticLoader):
        return "ticks"
    decoder = getattr(loader, "decoder", None)
    if decoder is None:
        return "records"
    if isinstance(decoder, CsvDecoder):
        return "rows"
    if isinstance(decoder, (JsonDecoder, JsonLinesDecoder, PickleDecoder)):
        return "items"
    return "records"


def build_source_label(loader: Any) -> str:
    producer = getattr(loader, "progress_label", None)
    if callable(producer):
        value = producer()
        text = str(value).strip() if value is not None else ""
        if text:
            return text

    if isinstance(loader, SyntheticLoader):
        generator_name = loader.generator.__class__.__name__
        return "generating data with " + generator_name
    if isinstance(loader, ForeachLoader):
        key = str(getattr(loader, "_key", "item"))
        values = getattr(loader, "_values", None)
        total = len(values) if isinstance(values, list) else "?"
        return f"fan-out {key}×{total}"

    if getattr(loader, "decoder", None) is None:
        return loader.__class__.__name__
    transport = getattr(loader, "transport", None)
    if isinstance(transport, (FsFileTransport, FsGlobTransport)):
        return "streaming from"
    if isinstance(transport, HttpTransport):
        return "downloading"
    return loader.__class__.__name__


def describe_loader(loader: Any) -> LoaderObservability:
    transport = getattr(loader, "transport", None)
    return LoaderObservability(
        transport=transport,
        glob_root=_transport_glob_root(transport),
    )


def loader_current_label(
    loader: Any,
    observability: LoaderObservability | None = None,
) -> str | None:
    if isinstance(loader, ForeachLoader):
        return _foreach_current_label(loader)
    observability = observability or describe_loader(loader)
    transport = observability.transport
    uri = getattr(loader, "current_resource_uri", None)
    if uri is None and isinstance(transport, FsFileTransport):
        uri = transport.path
    elif uri is None and isinstance(transport, HttpTransport):
        uri = transport.url
    return transport_resource_label(transport, uri, observability.glob_root)


def loader_current_resource_id(loader: Any) -> int | str | None:
    if isinstance(loader, ForeachLoader):
        return getattr(loader, "_current_index", None)
    return getattr(loader, "current_resource_uri", None)


def loader_progress_sequence(
    loader: Any,
    observability: LoaderObservability | None = None,
) -> list[SourceProgressEntry] | None:
    if isinstance(loader, ForeachLoader):
        return _foreach_progress_sequence(loader)
    observability = observability or describe_loader(loader)
    transport = observability.transport
    if isinstance(transport, FsGlobTransport):
        return _glob_progress_sequence(transport, observability.glob_root)
    return None


def transport_resource_label(
    transport: Any,
    uri: str | None,
    glob_root: Path | None = None,
) -> str | None:
    if not uri:
        return None
    if isinstance(transport, FsGlobTransport):
        return f'"{_relative_label(uri, glob_root)}"'
    if isinstance(transport, FsFileTransport):
        name = Path(uri).name or str(uri)
        return f'"{name}"'
    if isinstance(transport, HttpTransport):
        parts = urlparse(uri)
        return f"@{parts.netloc or 'http'}"
    return None


def _foreach_source_metadata(loader: ForeachLoader) -> dict[str, Any] | None:
    spec = getattr(loader, "_loader_spec", None)
    if not isinstance(spec, dict):
        return None
    args = spec.get("args")
    if foreach_transport_name(None, args) != "fs":
        return None
    values = getattr(loader, "_values", None)
    if not isinstance(values, list):
        return None

    labels = [foreach_value_label(value) for value in values]
    total = len(labels)
    metadata: dict[str, Any] = {"transport": "fs.glob", "count": total}
    if total == 1:
        metadata["file"] = labels[0]
    elif total > 1:
        metadata["first"] = labels[0]
        metadata["last"] = labels[-1]
    return metadata


def _glob_root(files: list[str]) -> Path | None:
    if not files:
        return None
    if len(files) == 1:
        return Path(files[0]).parent
    return Path(os.path.commonpath(files))


def _transport_glob_root(transport: Any) -> Path | None:
    if isinstance(transport, FsGlobTransport):
        return _glob_root(transport.files)
    return None


def _compact_root(path: Path, segments: int = 3) -> str:
    parts = [part for part in path.as_posix().split("/") if part]
    if len(parts) > segments:
        parts = ["...", *parts[-segments:]]
    return "/".join(parts) if parts else "/"


def _relative_label(path: str, root: Path | None) -> str:
    if root is not None:
        try:
            rel = Path(path).relative_to(root)
            rel_str = rel.as_posix()
            if rel_str:
                return rel_str
            return rel.name or path
        except (TypeError, ValueError):
            pass
    return Path(path).name or path


def foreach_transport_name(args: Any, spec_args: Any) -> str | None:
    if isinstance(args, dict):
        raw = args.get("transport")
        if raw is not None:
            return str(raw).strip().lower() or None
    if isinstance(spec_args, dict):
        raw = spec_args.get("transport")
        if raw is not None:
            return str(raw).strip().lower() or None
    return None


def _foreach_spec(loader: ForeachLoader) -> tuple[str, dict[str, Any] | None]:
    spec = getattr(loader, "_loader_spec", None)
    if not isinstance(spec, dict):
        return "", None
    entrypoint = str(spec.get("entrypoint", ""))
    maybe_args = spec.get("args")
    spec_args = maybe_args if isinstance(maybe_args, dict) else None
    return entrypoint, spec_args


def _foreach_current_label(loader: ForeachLoader) -> str | None:
    value = getattr(loader, "_current_value", None)
    if value is None:
        return None
    entrypoint, spec_args = _foreach_spec(loader)
    current_args = getattr(loader, "_current_args", None)
    action = foreach_action(entrypoint, current_args, spec_args)
    return join_action_value(action, foreach_value_label(value))


def _foreach_progress_sequence(
    loader: ForeachLoader,
) -> list[SourceProgressEntry] | None:
    values = getattr(loader, "_values", None)
    if not isinstance(values, list) or not values:
        return None
    entrypoint, spec_args = _foreach_spec(loader)
    action = foreach_action(entrypoint, None, spec_args)
    entries: list[SourceProgressEntry] = []
    for index, value in enumerate(values, start=1):
        value_label = foreach_value_label(value)
        label = join_action_value(action, value_label)
        entries.append(SourceProgressEntry(source_resource_id=index, label=label))
    return entries


def _glob_progress_sequence(
    transport: FsGlobTransport,
    glob_root: Path | None,
) -> list[SourceProgressEntry] | None:
    files = transport.files
    if not files:
        return None
    return [
        SourceProgressEntry(
            source_resource_id=path,
            label=f'"{_relative_label(path, glob_root)}"',
        )
        for path in files
    ]


def foreach_action(entrypoint: str, args: Any, spec_args: Any) -> str | None:
    if entrypoint == "core.io":
        transport_name = foreach_transport_name(args, spec_args)
        if transport_name == "http":
            return "downloading"
        if transport_name == "fs":
            return "streaming from"
        return None
    if entrypoint:
        return f"via {entrypoint}"
    return None


def foreach_value_label(value: Any) -> str:
    text = str(value)
    name = Path(text).name
    return name or text


def join_action_value(action: str | None, value_label: str) -> str:
    if action:
        return f'{action} "{value_label}"'
    return f'"{value_label}"'
