import logging
from dataclasses import dataclass
from typing import Any, Optional, Protocol, runtime_checkable

from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.adapters.fs import FsFileTransport, FsGlobTransport
from datapipeline.sources.adapters.http import HttpTransport
from datapipeline.sources.observability import (
    describe_loader,
    foreach_action,
    foreach_value_label,
    join_action_value,
)

from .common import (
    compute_glob_root,
    relative_label,
    resource_uri_label,
    transport_debug_lines,
    transport_info_lines,
)
from .execution_context import visible_dag_indent
from .labels import progress_meta_for_loader


@dataclass(frozen=True)
class ProgressSequenceEntry:
    label: str
    total: Optional[int]


@runtime_checkable
class LoaderBackedSource(Protocol):
    loader: Any


def supports_source_observability(stream_source: object) -> bool:
    return isinstance(stream_source, LoaderBackedSource)


class SourceObservabilityAdapter:
    def __init__(self, stream_source: LoaderBackedSource, stream_id: str):
        self.stream_id = stream_id
        if not supports_source_observability(stream_source):
            raise TypeError(f"Stream source '{stream_id}' must expose a loader")
        self.loader = stream_source.loader
        self._observability = describe_loader(self.loader)
        self._transport = self._observability.transport
        self._glob_root = (
            compute_glob_root(getattr(self._transport, "files", []))
            if isinstance(self._transport, FsGlobTransport)
            else None
        )
        self.description, _unit = progress_meta_for_loader(self.loader)

        prefix, sep, suffix = self.description.partition(": ")
        self._header = f"{prefix}:" if sep else self.description
        self._tail = suffix if sep else None
        self._is_foreach = isinstance(self.loader, ForeachLoader)
        self._progress_sequence_ready = False
        self._progress_sequence_cache: Optional[list[ProgressSequenceEntry]] = None

    def count(self) -> Optional[int]:
        try:
            return self.loader.count()
        except Exception:
            return None

    def input_streams(self) -> Optional[list[str]]:
        return self._observability.input_streams

    def input_detail_lines(self) -> list[str]:
        details = self.input_streams()
        if not details:
            return []
        parts: list[str] = []
        for entry in details:
            left, sep, right = str(entry).partition("=")
            if sep:
                parts.append(f"{left.strip()}={right.strip()}")
            else:
                text = str(entry).strip()
                if text:
                    parts.append(text)
        if not parts:
            return []
        return [f"Inputs: {', '.join(parts)}"]

    def current_label(self) -> Optional[str]:
        if self._is_foreach:
            return self._current_foreach_label()
        uri = getattr(self.loader, "current_resource_uri", None)
        if uri is None and isinstance(self._transport, FsFileTransport):
            uri = self._transport.path
        elif uri is None and isinstance(self._transport, HttpTransport):
            uri = self._transport.url
        return resource_uri_label(self._transport, uri, glob_root=self._glob_root)

    def progress_sequence(self) -> Optional[list[ProgressSequenceEntry]]:
        if self._progress_sequence_ready:
            return self._progress_sequence_cache
        entries: Optional[list[ProgressSequenceEntry]] = None
        if self._is_foreach:
            entries = self._foreach_progress_sequence()
        elif isinstance(self.loader, DataLoader):
            transport = getattr(self.loader, "transport", None)
            if isinstance(transport, FsGlobTransport):
                entries = self._glob_progress_sequence(transport)
        self._progress_sequence_cache = entries
        self._progress_sequence_ready = True
        return self._progress_sequence_cache

    def initial_label(self) -> Optional[str]:
        sequence = self.progress_sequence()
        if sequence:
            return sequence[0].label
        if self._is_foreach:
            values = getattr(self.loader, "_values", None)
            if isinstance(values, list) and values:
                entrypoint, spec_args = self._foreach_spec()
                current_args = getattr(self.loader, "_current_args", None)
                action = foreach_action(entrypoint, current_args, spec_args)
                first = str(values[0])
                first_label = foreach_value_label(first)
                return join_action_value(action, first_label)
        if isinstance(self._transport, FsGlobTransport):
            files = getattr(self._transport, "files", [])
            if files:
                return f"\"{relative_label(files[0], self._glob_root)}\""
        return self.current_label()

    def info_lines(self) -> list[str]:
        lines: list[str] = []
        lines.extend(self.input_detail_lines())
        if self._observability.info_lines:
            lines.extend(self._observability.info_lines)
        if self._include_transport_info():
            lines.extend(transport_info_lines(self._transport))
        return lines

    def debug_lines(self) -> list[str]:
        lines: list[str] = []
        if self._observability.debug_lines:
            lines.extend(self._observability.debug_lines)
        if self._include_transport_debug():
            lines.extend(transport_debug_lines(self._transport))
        return lines

    def current_indent(self, level: int = logging.INFO) -> str:
        return visible_dag_indent(level)

    def progress_visible(self) -> bool:
        producer = getattr(self.loader, "progress_visible", None)
        if not callable(producer):
            return True
        try:
            return bool(producer())
        except Exception:
            return True

    def _include_transport_info(self) -> bool:
        producer = getattr(self.loader, "include_transport_info", None)
        if not callable(producer):
            return True
        try:
            return bool(producer())
        except Exception:
            return True

    def _include_transport_debug(self) -> bool:
        producer = getattr(self.loader, "include_transport_debug", None)
        if not callable(producer):
            return True
        try:
            return bool(producer())
        except Exception:
            return True

    def format_label(
        self,
        name: Optional[str] = None,
        include_stream_id: bool = True,
        include_dag_indent: bool = True,
        level: int = logging.INFO,
    ) -> str:
        if name:
            if self._is_foreach:
                message = str(name)
            else:
                message = f"{self._header} {name}".rstrip()
        elif self._tail:
            message = f"{self._header} {self._tail}".rstrip()
        else:
            message = self.description
        indent = self.current_indent(level) if include_dag_indent else ""
        if include_stream_id:
            return f"{indent}[{self.stream_id}] {message}"
        return f"{indent}{message}"

    def _foreach_spec(self) -> tuple[str, Optional[dict[str, Any]]]:
        spec = getattr(self.loader, "_loader_spec", None)
        if not isinstance(spec, dict):
            return "", None
        entrypoint = str(spec.get("entrypoint", ""))
        maybe_args = spec.get("args")
        spec_args = maybe_args if isinstance(maybe_args, dict) else None
        return entrypoint, spec_args

    def _current_foreach_label(self) -> Optional[str]:
        value = getattr(self.loader, "_current_value", None)
        if value is None:
            return None
        entrypoint, spec_args = self._foreach_spec()
        current_args = getattr(self.loader, "_current_args", None)
        action = foreach_action(entrypoint, current_args, spec_args)
        return join_action_value(action, foreach_value_label(value))

    def _foreach_progress_sequence(self) -> Optional[list[ProgressSequenceEntry]]:
        loader = self.loader
        if not isinstance(loader, ForeachLoader):
            return None
        values = getattr(loader, "_values", None)
        if not isinstance(values, list) or not values:
            return None
        entrypoint, spec_args = self._foreach_spec()
        action = foreach_action(entrypoint, None, spec_args)
        entries: list[ProgressSequenceEntry] = []
        for value in values:
            value_text = str(value)
            value_label = foreach_value_label(value_text)
            label = join_action_value(action, value_label)
            entries.append(ProgressSequenceEntry(label=label, total=None))
        return entries

    def _glob_progress_sequence(
        self,
        transport: FsGlobTransport,
    ) -> Optional[list[ProgressSequenceEntry]]:
        files = list(getattr(transport, "files", []))
        if not files:
            return None
        glob_root = compute_glob_root(files)
        entries: list[ProgressSequenceEntry] = []
        for path in files:
            label = f"\"{relative_label(path, glob_root)}\""
            entries.append(ProgressSequenceEntry(label=label, total=None))
        return entries
