from typing import Any, Optional

from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.transports import FsGlobTransport

from .common import (
    compute_glob_root,
    current_loader_label,
    log_combined_stream,
    transport_debug_lines,
    transport_info_lines,
)
from .labels import progress_meta_for_loader

class SourceObservabilityAdapter:
    def __init__(self, stream_source: Any, stream_id: str):
        self.stream_id = stream_id
        self.loader = getattr(stream_source, "loader", None)
        if self.loader is None:
            raise TypeError(f"Stream source '{stream_id}' must expose a loader")
        self.transport = getattr(self.loader, "transport", None)
        self.description, self.unit = progress_meta_for_loader(self.loader)

        prefix, sep, suffix = self.description.partition(": ")
        self._header = f"{prefix}:" if sep else self.description
        self._tail = suffix if sep else None
        self._is_foreach = isinstance(self.loader, ForeachLoader)
        self._glob_root = None
        if isinstance(self.transport, FsGlobTransport):
            self._glob_root = compute_glob_root(getattr(self.transport, "files", []))

    def count(self) -> Optional[int]:
        try:
            return self.loader.count()
        except Exception:
            return None

    def composed_inputs(self) -> Optional[list[str]]:
        try:
            spec = getattr(self.loader, "_spec", None)
            inputs = getattr(spec, "inputs", None)
            if isinstance(inputs, (list, tuple)) and inputs:
                return [str(item) for item in inputs]
        except Exception:
            return None
        return None

    def log_composed_details(self) -> None:
        log_combined_stream(self.stream_id, self.composed_inputs())

    def current_label(self) -> Optional[str]:
        return current_loader_label(
            self.loader,
            self.transport,
            glob_root=self._glob_root,
        )

    def info_lines(self) -> list[str]:
        return transport_info_lines(self.transport)

    def debug_lines(self) -> list[str]:
        return transport_debug_lines(self.transport)

    def preparing_label(self) -> str:
        return f"[{self.stream_id}] Preparing data stream"

    def format_label(self, name: Optional[str] = None, include_stream_id: bool = True) -> str:
        if name:
            if self._is_foreach:
                message = str(name)
            else:
                message = f"{self._header} {name}".rstrip()
        elif self._tail:
            message = f"{self._header} {self._tail}".rstrip()
        else:
            message = self.description
        if include_stream_id:
            return f"[{self.stream_id}] {message}"
        return message
