import logging
from typing import Any, Optional, Protocol, runtime_checkable

from datapipeline.sources.foreach import ForeachLoader
from datapipeline.sources.observability import (
    SourceProgressEntry,
    describe_loader,
    loader_current_label,
    loader_progress_sequence,
    progress_meta_for_loader,
    transport_debug_lines,
)
from .execution_context import visible_source_indent


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
        self.description, _unit = progress_meta_for_loader(self.loader)

        prefix, sep, suffix = self.description.partition(": ")
        self._header = f"{prefix}:" if sep else self.description
        self._tail = suffix if sep else None
        self._is_foreach = isinstance(self.loader, ForeachLoader)
        self._progress_sequence_ready = False
        self._progress_sequence_cache: Optional[list[SourceProgressEntry]] = None

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
        return loader_current_label(self.loader, self._observability)

    def progress_sequence(self) -> Optional[list[SourceProgressEntry]]:
        if self._progress_sequence_ready:
            return self._progress_sequence_cache
        self._progress_sequence_cache = loader_progress_sequence(
            self.loader,
            self._observability,
        )
        self._progress_sequence_ready = True
        return self._progress_sequence_cache

    def initial_label(self) -> Optional[str]:
        sequence = self.progress_sequence()
        if sequence:
            return sequence[0].label
        return self.current_label()

    def info_lines(self) -> list[str]:
        lines: list[str] = []
        lines.extend(self.input_detail_lines())
        if self._observability.info_lines:
            lines.extend(self._observability.info_lines)
        return lines

    def debug_lines(self) -> list[str]:
        lines: list[str] = []
        if self._observability.debug_lines:
            lines.extend(self._observability.debug_lines)
        if self._include_transport_debug():
            lines.extend(transport_debug_lines(self._observability.transport))
        return lines

    def current_indent(self, level: int = logging.INFO) -> str:
        return visible_source_indent(level)

    def progress_visible(self) -> bool:
        producer = getattr(self.loader, "progress_visible", None)
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
