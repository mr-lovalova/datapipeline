from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from datapipeline.config.tasks import ServeOutputConfig
from datapipeline.services.runs import RunPaths, start_run_for_directory


def _format_suffix(fmt: str) -> str:
    suffix_map = {
        "json-lines": ".jsonl",
        "json": ".json",
        "csv": ".csv",
        "pickle": ".pkl",
    }
    return suffix_map.get(fmt, ".out")


def _default_filename_for_format(fmt: str) -> str:
    suffix = _format_suffix(fmt)
    return f"vectors{suffix}"


def _sanitize_segment(value: str) -> str:
    cleaned = "".join(
        ch if ch.isalnum() or ch in ("_", "-", ".") else "_"
        for ch in value.strip()
    )
    return cleaned or "run"


@dataclass(frozen=True)
class OutputTarget:
    """Resolved writer target describing how and where to emit records."""

    transport: str  # stdout | fs
    format: str     # print | json-lines | json | csv | pickle
    destination: Optional[Path]
    payload: str = "sample"
    run: RunPaths | None = None

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
        return OutputTarget(
            transport=self.transport,
            format=self.format,
            destination=new_path,
            payload=self.payload,
            run=self.run,
        )


class OutputResolutionError(ValueError):
    """Raised when CLI/config output options cannot be resolved."""


def resolve_output_target(
    *,
    cli_output: ServeOutputConfig | None,
    config_output: ServeOutputConfig | None,
    default: ServeOutputConfig | None = None,
    base_path: Path | None = None,
    run_name: str | None = None,
    payload_override: str | None = None,
    stage: int | None = None,
    create_run: bool = False,
) -> OutputTarget:
    """
    Resolve the effective output target using CLI override, run config, or default.
    """

    base_path = base_path or Path.cwd()

    config = cli_output or config_output or default
    if config is None:
        config = ServeOutputConfig(transport="stdout", format="print")

    payload = payload_override or config.payload or "sample"

    if config.transport == "stdout":
        return OutputTarget(
            transport="stdout",
            format=config.format,
            destination=None,
            payload=payload,
            run=None,
        )

    if config.directory is None:
        raise OutputResolutionError("fs output requires a directory")
    directory = (
        config.directory
        if config.directory.is_absolute()
        else (base_path / config.directory).resolve()
    )
    if create_run:
        run_paths, _ = start_run_for_directory(directory, stage=stage)
        base_dest_dir = run_paths.dataset_dir
    else:
        run_paths = None
        # When not creating a managed run, nest outputs under an optional
        # run_name subdirectory to keep layouts consistent with tests/CLI.
        base_dest_dir = directory
        if run_name:
            base_dest_dir = base_dest_dir / _sanitize_segment(run_name)
    suffix = _format_suffix(config.format)
    filename_stem = config.filename or run_name
    if filename_stem:
        filename = f"{filename_stem}{suffix}"
    else:
        filename = _default_filename_for_format(config.format)
    dest_path = (base_dest_dir / filename).resolve()

    return OutputTarget(
        transport="fs",
        format=config.format,
        destination=dest_path,
        payload=payload,
        run=run_paths,
    )
