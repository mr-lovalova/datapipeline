from dataclasses import dataclass
from typing import Any, Optional
import logging

from datapipeline.sources.data_loader import DataLoader
from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.adapters.fs import FsGlobTransport
from datapipeline.sources.observability import (
    describe_loader,
    foreach_action,
    foreach_value_label,
    join_action_value,
)

from .common import (
    compute_glob_root,
    current_transport_label,
    relative_label,
    log_combined_stream,
    transport_debug_lines,
    transport_info_lines,
)
from .execution_context import visible_dag_indent
from .labels import progress_meta_for_loader


@dataclass(frozen=True)
class ProgressSequenceEntry:
    label: str
    total: Optional[int]


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
        self._progress_sequence_ready = False
        self._progress_sequence_cache: Optional[list[ProgressSequenceEntry]] = None

    def count(self) -> Optional[int]:
        try:
            return self.loader.count()
        except Exception:
            return None

    def composed_inputs(self) -> Optional[list[str]]:
        return describe_loader(self.loader).composed_inputs

    def log_composed_details(self, level: int = logging.INFO) -> None:
        details = self.composed_inputs()
        if not details:
            return
        log_combined_stream(
            self.stream_id,
            details,
            indent=self.current_indent(level),
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

    def progress_sequence(self) -> Optional[list[ProgressSequenceEntry]]:
        if self._progress_sequence_ready:
            return self._progress_sequence_cache
        entries: Optional[list[ProgressSequenceEntry]] = None
        if self._is_foreach:
            entries = self._foreach_progress_sequence()
        elif isinstance(self.loader, DataLoader):
            transport = getattr(self.loader, "transport", None)
            if isinstance(transport, FsGlobTransport):
                entries = self._glob_progress_sequence(self.loader, transport)
        self._progress_sequence_cache = entries
        self._progress_sequence_ready = True
        return self._progress_sequence_cache

    def initial_label(self) -> Optional[str]:
        sequence = self.progress_sequence()
        if sequence:
            return sequence[0].label
        observability = describe_loader(self.loader)
        if observability.current_label:
            return observability.current_label
        if self._is_foreach:
            values = getattr(self.loader, "_values", None)
            if isinstance(values, list) and values:
                entrypoint, spec_args = self._foreach_spec()
                current_args = getattr(self.loader, "_current_args", None)
                action = foreach_action(entrypoint, current_args, spec_args)
                first = str(values[0])
                first_label = foreach_value_label(first)
                return join_action_value(action, first_label)
        transport = observability.transport
        if isinstance(transport, FsGlobTransport):
            files = getattr(transport, "files", [])
            if files:
                glob_root = compute_glob_root(files)
                return f"\"{relative_label(files[0], glob_root)}\""
        return current_transport_label(transport)

    def info_lines(self) -> list[str]:
        observability = describe_loader(self.loader)
        lines: list[str] = []
        if observability.info_lines:
            lines.extend(observability.info_lines)
        lines.extend(transport_info_lines(observability.transport))
        return lines

    def debug_lines(self) -> list[str]:
        observability = describe_loader(self.loader)
        lines: list[str] = []
        if observability.debug_lines:
            lines.extend(observability.debug_lines)
        lines.extend(transport_debug_lines(observability.transport))
        return lines

    def current_indent(self, level: int = logging.INFO) -> str:
        return visible_dag_indent(level)

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

    def _foreach_progress_sequence(self) -> Optional[list[ProgressSequenceEntry]]:
        loader = self.loader
        if not isinstance(loader, ForeachLoader):
            return None
        values = getattr(loader, "_values", None)
        if not isinstance(values, list) or not values:
            return None
        key = str(getattr(loader, "_key", "value"))
        entrypoint, spec_args = self._foreach_spec()
        action = foreach_action(entrypoint, None, spec_args)
        entries: list[ProgressSequenceEntry] = []
        for value in values:
            value_text = str(value)
            value_label = foreach_value_label(value_text)
            label = join_action_value(action, value_label)
            total: Optional[int] = None
            try:
                vars_ = {key: value}
                loader_args = loader._make_loader_args(vars_)  # type: ignore[attr-defined]
                child_loader = loader._build_loader(loader_args)  # type: ignore[attr-defined]
                count_value = child_loader.count()
                if count_value is not None:
                    total = int(count_value)
            except Exception:
                total = None
            entries.append(ProgressSequenceEntry(label=label, total=total))
        return entries

    @staticmethod
    def _count_fs_file(
        path: str,
        
        chunk_size: int,
        loader: DataLoader,
    ) -> Optional[int]:
        def _chunks():
            with open(path, "rb") as handle:
                while True:
                    chunk = handle.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

        try:
            count_value = loader.decoder.count(_chunks())
            if count_value is None:
                return None
            return int(count_value)
        except Exception:
            return None

    def _glob_progress_sequence(
        self,
        loader: DataLoader,
        transport: FsGlobTransport,
    ) -> Optional[list[ProgressSequenceEntry]]:
        files = list(getattr(transport, "files", []))
        if not files:
            return None
        glob_root = compute_glob_root(files)
        chunk_size = int(getattr(transport, "chunk_size", 65536) or 65536)
        entries: list[ProgressSequenceEntry] = []
        for path in files:
            label = f"\"{relative_label(path, glob_root)}\""
            total = self._count_fs_file(path, chunk_size=chunk_size, loader=loader)
            entries.append(ProgressSequenceEntry(label=label, total=total))
        return entries
