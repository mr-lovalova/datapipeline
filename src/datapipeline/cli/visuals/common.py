from pathlib import Path
import logging
import os
from collections.abc import Sequence

from urllib.parse import urlparse
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport

logger = logging.getLogger(__name__)


def compute_glob_root(files: list[str]) -> Path | None:
    if not files:
        return None
    try:
        return Path(os.path.commonpath(files))
    except (TypeError, ValueError):
        return None


def compact_root(path: Path, segments: int = 3) -> str:
    parts = [part for part in path.as_posix().split("/") if part]
    if len(parts) > segments:
        parts = ["..."] + parts[-segments:]
    return "/".join(parts) if parts else "/"


def relative_label(path: str, root: Path | None) -> str:
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


def _fs_glob_info_lines(transport: FsGlobTransport) -> list[str]:
    pattern = getattr(transport, "pattern", "")
    files = transport.files
    total = len(files)
    # Build INFO-level summary lines
    lines: list[str] = []
    root = compute_glob_root(files)
    root_label = compact_root(root) if root else pattern or "fs"
    if total == 0:
        lines.append(f"fs.glob: count=0 root={root_label}")
    elif total == 1:
        rel = relative_label(files[0], root)
        lines.append(f"fs.glob: count=1 file={rel}")
    else:
        rel_first = relative_label(files[0], root)
        rel_last = relative_label(files[-1], root)
        lines.append(f"fs.glob: count={total} first={rel_first} last={rel_last}")
    return lines


def _fs_file_info_lines(transport: FsFileTransport) -> list[str]:
    path = getattr(transport, "path", "")
    name = Path(path).name or str(path)
    return [f"fs.file: file={name}"]


def _http_info_lines(transport: HttpTransport) -> list[str]:
    url = getattr(transport, "url", "")
    try:
        parts = urlparse(url)
        host = parts.netloc or "http"
        resource = Path(parts.path or "").name
    except Exception:
        host = "http"
        resource = ""
    line = f"http.fetch: host={host}"
    if resource:
        line = f"{line} resource={resource}"
    return [line]


def transport_info_lines(transport) -> list[str]:
    """Return INFO-level summary lines for a transport without logging.

    Used to render grouped per-source summaries alongside completion messages.
    """
    if isinstance(transport, FsGlobTransport):
        return _fs_glob_info_lines(transport)
    if isinstance(transport, FsFileTransport):
        return _fs_file_info_lines(transport)
    if isinstance(transport, HttpTransport):
        return _http_info_lines(transport)
    return []


def transport_debug_lines(transport) -> list[str]:
    """Return DEBUG-level detail lines without logging.

    Used by rich visuals to render debug details inside the Live layout to
    avoid pushing progress bars when logs are emitted.
    """
    lines: list[str] = []
    if isinstance(transport, FsGlobTransport):
        files = getattr(transport, "files", [])
        total = len(files)
        if total > 0:
            root = compute_glob_root(files)
            rel_files = [relative_label(path, root) for path in files]
            for idx, path in enumerate(rel_files, start=1):
                lines.append(f"fs.glob file {idx}/{total}: {path}")
        return lines
    if isinstance(transport, FsFileTransport):
        # INFO line already conveys the filename; avoid redundant DEBUG line
        return lines
    if isinstance(transport, HttpTransport):
        url = getattr(transport, "url", "")
        if url:
            lines.append(f"http full: {url}")
        return lines
    return lines


def log_combined_stream(
    stream_id: str,
    details: Sequence[str] | str | None,
    indent: str = "",
) -> None:
    """Emit descriptive logs for virtual multi-input sources."""

    entries: list[str] = []
    if isinstance(details, str):
        entries = [part.strip() for part in details.split(",") if part.strip()]
    elif isinstance(details, Sequence):
        entries = [str(part).strip() for part in details if str(part).strip()]
    elif details is not None:
        item = str(details).strip()
        if item:
            entries = [item]

    if entries:
        logger.info("%s[%s] Feature engineering from:", indent, stream_id)
        for entry in entries:
            left, sep, right = entry.partition("=")
            if sep:
                mapping = f"{left.strip()} -> {right.strip()}"
            else:
                mapping = entry
            logger.info("%s[%s]   - %s", indent, stream_id, mapping)
    else:
        logger.info("%s[%s] Feature engineering from upstream inputs", indent, stream_id)


def current_transport_label(transport, glob_root: Path | None = None) -> str | None:
    """Return a human-friendly label for the transport's current unit of work."""
    if isinstance(transport, FsGlobTransport):
        current = getattr(transport, "current_path", None)
        if not current:
            return None
        return f"\"{relative_label(current, glob_root)}\""
    if isinstance(transport, FsFileTransport):
        path = getattr(transport, "path", None)
        if not path:
            return None
        try:
            name = Path(path).name or str(path)
            return f"\"{name}\""
        except Exception:
            return f"\"{path}\""
    if isinstance(transport, HttpTransport):
        url = getattr(transport, "url", None)
        if not url:
            return None
        try:
            parts = urlparse(url)
            host = parts.netloc or "http"
            return f"@{host}"
        except Exception:
            return None
    return None
