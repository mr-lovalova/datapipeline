from pathlib import Path
from typing import Protocol, runtime_checkable


@runtime_checkable
class Writer(Protocol):
    def write(self, item: object) -> None: ...
    def close(self) -> None: ...
    def abort(self) -> None: ...


@runtime_checkable
class HeaderCapable(Protocol):
    """Writers that can accept an injected logical 'header record' as the first write."""

    def write_header(self, header: dict) -> None: ...


@runtime_checkable
class HasFilePath(Protocol):
    @property
    def file_path(self) -> Path | None: ...
