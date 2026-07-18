from dataclasses import dataclass, replace
from pathlib import Path
from unicodedata import normalize

from datapipeline.config.profiles import Format, ServeOutputConfig, Transport, View
from datapipeline.io.runs import RunPaths
from datapipeline.services.path_policy import (
    resolve_relative_to_base,
    sanitize_path_segment,
    workspace_cwd,
)


def _format_suffix(fmt: Format) -> str:
    suffix_map = {
        "jsonl": ".jsonl",
        "csv": ".csv",
        "pickle": ".pkl",
        "txt": ".txt",
        "html": ".html",
    }
    return suffix_map[fmt]


def _default_view_for_format(fmt: Format) -> View:
    if fmt == "csv":
        return "flat"
    return "raw"


def _resolve_view(fmt: Format, configured_view: View | None) -> View:
    return configured_view or _default_view_for_format(fmt)


def _default_filename_for_format(fmt: Format) -> str:
    suffix = _format_suffix(fmt)
    return f"vectors{suffix}"


@dataclass(frozen=True)
class OutputTarget:
    """Resolved writer target describing how and where to emit records."""

    transport: Transport
    format: Format
    view: View
    encoding: str | None
    destination: Path | None
    run: RunPaths | None = None

    def for_feature(self, feature_id: str) -> "OutputTarget":
        if self.transport != "fs" or self.destination is None:
            return self
        safe_feature = sanitize_path_segment(str(feature_id), default="feature")
        dest = self.destination
        suffix = _format_suffix(self.format)
        stem = dest.name.removesuffix(suffix)
        new_name = f"{stem}.{safe_feature}{suffix}"
        new_path = dest.with_name(new_name)
        return replace(self, destination=new_path)

    def for_output(self, output_id: str) -> "OutputTarget":
        if self.transport != "fs" or self.destination is None:
            return self
        safe_output_id = sanitize_path_segment(output_id)
        dest = self.destination
        suffix = _format_suffix(self.format)
        stem = dest.name.removesuffix(suffix)
        new_name = f"{stem}.{safe_output_id}{suffix}"
        new_path = dest.with_name(new_name)
        return replace(self, destination=new_path)


def output_destination_key(path: Path) -> str:
    """Return the portable identity used to reject colliding output paths."""
    return normalize("NFC", str(path)).casefold()


class OutputResolutionError(ValueError):
    """Raised when CLI/config output options cannot be resolved."""


def resolve_output_directory(
    config: ServeOutputConfig | None,
    *,
    base_path: Path | None = None,
) -> Path | None:
    """Resolve the filesystem output directory when the target uses fs transport."""
    if config is None or config.transport != "fs" or config.directory is None:
        return None
    base = base_path or workspace_cwd()
    return resolve_relative_to_base(config.directory, base)


def resolve_output_target(
    cli_output: ServeOutputConfig | None,
    config_output: ServeOutputConfig | None,
    default: ServeOutputConfig | None = None,
    base_path: Path | None = None,
    profile_name: str | None = None,
    run_paths: RunPaths | None = None,
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
    directory = resolve_relative_to_base(
        config.directory,
        base_path,
    )
    if run_paths is not None:
        base_dest_dir = run_paths.dataset_dir
    else:
        base_dest_dir = directory
        if profile_name:
            base_dest_dir = base_dest_dir / sanitize_path_segment(profile_name)
    suffix = _format_suffix(config.format)
    filename_stem = config.filename or (
        sanitize_path_segment(profile_name) if profile_name else None
    )
    if filename_stem:
        if Path(filename_stem).suffix == suffix:
            raise OutputResolutionError(f"filename must omit the '{suffix}' extension")
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
