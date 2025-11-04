from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from datapipeline.config.run import OutputConfig


@dataclass(frozen=True)
class OutputTarget:
    """Resolved writer target describing how and where to emit records."""

    transport: str  # stdout | fs
    format: str     # print | json-lines | json | csv | pickle
    destination: Optional[Path]

    def for_feature(self, feature_id: str) -> "OutputTarget":
        if self.transport != "fs" or self.destination is None:
            return self
        safe_feature = "".join(
            ch if ch.isalnum() or ch in ("_", "-", ".") else "_"
            for ch in str(feature_id)
        )
        dest = self.destination
        suffix = "".join(dest.suffixes)
        stem = dest.name[: -len(suffix)] if suffix else dest.name
        new_name = f"{stem}.{safe_feature}{suffix}"
        new_path = dest.with_name(new_name)
        return OutputTarget(transport=self.transport, format=self.format, destination=new_path)


class OutputResolutionError(ValueError):
    """Raised when CLI/config output options cannot be resolved."""


def resolve_output_target(
    *,
    cli_output: OutputConfig | None,
    config_output: OutputConfig | None,
    default: OutputConfig | None = None,
    base_path: Path | None = None,
) -> OutputTarget:
    """
    Resolve the effective output target using CLI override, run config, or default.
    """

    base_path = base_path or Path.cwd()

    config = cli_output or config_output or default
    if config is None:
        config = OutputConfig(transport="stdout", format="print")

    if config.transport == "stdout":
        return OutputTarget(
            transport="stdout",
            format=config.format,
            destination=None,
        )

    dest_path = config.path
    if dest_path is None:
        raise OutputResolutionError("fs output requires a destination path")
    if not dest_path.is_absolute():
        dest_path = (base_path / dest_path).resolve()

    return OutputTarget(
        transport="fs",
        format=config.format,
        destination=dest_path,
    )
