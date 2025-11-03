from typing import Protocol, Optional, runtime_checkable
from pathlib import Path


@runtime_checkable
class Writer(Protocol):
    def write(self, rec: dict) -> None: ...
    def close(self) -> None: ...


@runtime_checkable
class HeaderCapable(Protocol):
    """Writers that can accept an injected logical 'header record' as the first write."""

    def write_header(self, header: dict) -> None: ...


@runtime_checkable
class HasFilePath(Protocol):
    @property
    def file_path(self) -> Optional[Path]: ...
