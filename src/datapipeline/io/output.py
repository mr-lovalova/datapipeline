from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from datapipeline.config.profiles import ServeOutputConfig
from datapipeline.services.path_policy import (
    resolve_relative_to_base,
    sanitize_path_segment,
    workspace_cwd,
)
from datapipeline.services.runs import RunPaths, start_run_for_directory


def _format_suffix(fmt: str) -> str:
    suffix_map = {
        "jsonl": ".jsonl",
        "csv": ".csv",
        "pickle": ".pkl",
        "txt": ".txt",
        "html": ".html",
    }
    return suffix_map.get(fmt, ".out")


def _default_view_for_format(fmt: str) -> str:
    if fmt == "jsonl":
        return "raw"
    return "flat"


def _resolve_view(fmt: str, configured_view: str | None) -> str:
    return configured_view or _default_view_for_format(fmt)


def _default_filename_for_format(fmt: str) -> str:
    suffix = _format_suffix(fmt)
    return f"vectors{suffix}"


@dataclass(frozen=True)
class OutputTarget:
    """Resolved writer target describing how and where to emit records."""

    transport: str  # stdout | fs
    format: str     # jsonl | csv | pickle | html
    view: str       # flat | raw | values
    encoding: str | None
    destination: Optional[Path]
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
            view=self.view,
            encoding=self.encoding,
            destination=new_path,
            run=self.run,
        )


class OutputResolutionError(ValueError):
    """Raised when CLI/config output options cannot be resolved."""


def resolve_destination(
    target: OutputTarget | None,
    *,
    base_dir: Path,
    default_filename: str,
) -> Path:
    if target is not None and target.destination is not None:
        return target.destination.resolve()
    return (base_dir / default_filename).resolve()


def served_output_message(target: OutputTarget, count: int) -> str:
    if target.destination:
        return f"Saved {count} items: {target.destination}"
    if target.transport == "stdout":
        return f"Streamed {count} items: stdout"
    return f"Emitted {count} items"


def materialized_output_message(
    artifact_key: str,
    path: Path,
    *,
    meta: dict[str, object] | None = None,
) -> str:
    if not meta:
        return f"Materialized {artifact_key}: {path}"
    details = ", ".join(f"{k}={v}" for k, v in meta.items())
    return f"Materialized {artifact_key}: {path} ({details})"


def resolve_output_target(
    
    cli_output: ServeOutputConfig | None,
    config_output: ServeOutputConfig | None,
    default: ServeOutputConfig | None = None,
    base_path: Path | None = None,
    run_name: str | None = None,
    stage: int | None = None,
    create_run: bool = False,
) -> OutputTarget:
    """
    Resolve the effective output target using CLI override, run config, or default.
    """

    base_path = base_path or workspace_cwd()

    config = cli_output or config_output or default
    if config is None:
        config = ServeOutputConfig(transport="stdout", format="jsonl")

    if config.transport == "stdout":
        return OutputTarget(
            transport="stdout",
            format=config.format,
            view=_resolve_view(config.format, config.view),
            encoding=None,
            destination=None,
            run=None,
        )

    if config.directory is None:
        raise OutputResolutionError("fs output requires a directory")
    directory = resolve_relative_to_base(config.directory, base_path, resolve=True)
    if create_run:
        run_paths, _ = start_run_for_directory(directory, stage=stage)
        base_dest_dir = run_paths.dataset_dir
    else:
        run_paths = None
        # When not creating a managed run, nest outputs under an optional
        # run_name subdirectory to keep layouts consistent with tests/CLI.
        base_dest_dir = directory
        if run_name:
            base_dest_dir = base_dest_dir / sanitize_path_segment(run_name)
    suffix = _format_suffix(config.format)
    filename_stem = config.filename or (
        sanitize_path_segment(run_name) if run_name else None
    )
    if filename_stem:
        filename = f"{filename_stem}{suffix}"
    else:
        filename = _default_filename_for_format(config.format)
    dest_path = (base_dest_dir / filename).resolve()

    return OutputTarget(
        transport="fs",
        format=config.format,
        view=_resolve_view(config.format, config.view),
        encoding=config.encoding,
        destination=dest_path,
        run=run_paths,
    )
