import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from datapipeline.domain.stream import RecordStream
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport
from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.models.loader import BaseDataLoader, SourceProgressUnit
from datapipeline.sources.models.source import Source
from datapipeline.sources.ports import SourceTransport


@dataclass(frozen=True)
class SourceProgressEntry:
    source_resource_id: int | str
    label: str


@dataclass(frozen=True)
class LoaderObservability:
    loader: BaseDataLoader
    transport: SourceTransport | None
    glob_root: Path | None
    progress_sequence: tuple[SourceProgressEntry, ...] | None

    @property
    def unit(self) -> SourceProgressUnit:
        return self.loader.progress_unit

    @property
    def current_resource_id(self) -> str | None:
        return self.loader.current_resource_uri

    @property
    def current_label(self) -> str | None:
        uri = self.current_resource_id
        if uri is None and isinstance(self.transport, FsFileTransport):
            uri = self.transport.path
        elif uri is None and isinstance(self.transport, HttpTransport):
            uri = self.transport.url
        return _transport_resource_label(self.transport, uri, self.glob_root)


def source_summary(stream_source: RecordStream[object]) -> str | None:
    if not isinstance(stream_source, Source):
        return None
    loader = stream_source.loader
    if not isinstance(loader, DataLoader):
        return None
    return _transport_source_summary(loader.transport)


def _transport_source_summary(transport: SourceTransport) -> str | None:
    if isinstance(transport, FsFileTransport):
        path = transport.path
        return f"transport=fs.file file={Path(path).name or str(path)}"

    if isinstance(transport, FsGlobTransport):
        files = transport.files
        total = len(files)
        parts = ["transport=fs.glob", f"count={total}"]
        root = _glob_root(files)
        if total == 0:
            pattern = transport.pattern
            parts.append(f"root={_compact_root(root) if root else pattern or 'fs'}")
        elif total == 1:
            parts.append(f"file={_relative_label(files[0], root)}")
        else:
            parts.append(f"first={_relative_label(files[0], root)}")
            parts.append(f"last={_relative_label(files[-1], root)}")
        return " ".join(parts)

    if isinstance(transport, HttpTransport):
        url = transport.url
        parsed_url = urlparse(url)
        host = parsed_url.netloc or "http"
        resource = Path(parsed_url.path or "").name
        summary = f"transport=http.fetch host={host}"
        return f"{summary} resource={resource}" if resource else summary

    return None


def describe_loader(loader: BaseDataLoader) -> LoaderObservability:
    transport = loader.transport if isinstance(loader, DataLoader) else None
    glob_root = None
    progress_sequence = None
    if isinstance(transport, FsGlobTransport):
        glob_root = _glob_root(transport.files)
        progress_sequence = _glob_progress_sequence(transport, glob_root)
    return LoaderObservability(
        loader=loader,
        transport=transport,
        glob_root=glob_root,
        progress_sequence=progress_sequence,
    )


def _transport_resource_label(
    transport: SourceTransport | None,
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


def _glob_root(files: list[str]) -> Path | None:
    if not files:
        return None
    if len(files) == 1:
        return Path(files[0]).parent
    return Path(os.path.commonpath(files))


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
        except ValueError:
            return Path(path).name or path
    return Path(path).name or path


def _glob_progress_sequence(
    transport: FsGlobTransport,
    glob_root: Path | None,
) -> tuple[SourceProgressEntry, ...] | None:
    files = transport.files
    if not files:
        return None
    return tuple(
        SourceProgressEntry(
            source_resource_id=path,
            label=f'"{_relative_label(path, glob_root)}"',
        )
        for path in files
    )
