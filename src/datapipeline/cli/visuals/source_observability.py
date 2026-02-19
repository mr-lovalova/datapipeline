from typing import Any, Optional

from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.transports import FsGlobTransport
from datapipeline.sources.observability import describe_loader

from .common import (
    compute_glob_root,
    current_transport_label,
    log_combined_stream,
    transport_debug_lines,
    transport_info_lines,
)
from .execution_context import current_dag_indent
from .labels import progress_meta_for_loader

class SourceObservabilityAdapter:
    def __init__(self, stream_source: Any, stream_id: str):
        self.stream_id = stream_id
        self.loader = getattr(stream_source, "loader", None)
        if self.loader is None:
            raise TypeError(f"Stream source '{stream_id}' must expose a loader")
        self.description, self.unit = progress_meta_for_loader(self.loader)

        prefix, sep, suffix = self.description.partition(": ")
        self._header = f"{prefix}:" if sep else self.description
        self._tail = suffix if sep else None
        self._is_foreach = isinstance(self.loader, ForeachLoader)

    def count(self) -> Optional[int]:
        try:
            return self.loader.count()
        except Exception:
            return None

    def composed_inputs(self) -> Optional[list[str]]:
        return describe_loader(self.loader).composed_inputs

    def log_composed_details(self) -> None:
        details = self.composed_inputs()
        if not details:
            return
        log_combined_stream(
            self.stream_id,
            details,
            indent=self.current_indent(),
        )

    def current_label(self) -> Optional[str]:
        observability = describe_loader(self.loader)
        if observability.current_label:
            return observability.current_label
        transport = observability.transport
        glob_root = None
        if isinstance(transport, FsGlobTransport):
            glob_root = compute_glob_root(getattr(transport, "files", []))
        return current_transport_label(transport, glob_root=glob_root)

    def info_lines(self) -> list[str]:
        return transport_info_lines(describe_loader(self.loader).transport)

    def debug_lines(self) -> list[str]:
        return transport_debug_lines(describe_loader(self.loader).transport)

    def current_indent(self) -> str:
        return current_dag_indent()

    def format_label(self, name: Optional[str] = None, include_stream_id: bool = True, include_dag_indent: bool = True) -> str:
        if name:
            if self._is_foreach:
                message = str(name)
            else:
                message = f"{self._header} {name}".rstrip()
        elif self._tail:
            message = f"{self._header} {self._tail}".rstrip()
        else:
            message = self.description
        indent = self.current_indent() if include_dag_indent else ""
        if include_stream_id:
            return f"{indent}[{self.stream_id}] {message}"
        return f"{indent}{message}"
