from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from datapipeline.config.run import OutputConfig


@dataclass(frozen=True)
class OutputTarget:
    """Resolved writer target describing how and where to emit records."""

    writer_kind: str
    destination: Optional[Path]


class OutputResolutionError(ValueError):
    """Raised when CLI/config output options cannot be resolved."""


def _path_writer_for(path: Path) -> str:
    suffix = "".join(path.suffixes).lower()
    if suffix.endswith(".jsonl.gz") or suffix.endswith(".json.gz") or suffix.endswith(".gz"):
        return "file.jsonl.gz"
    if suffix.endswith(".jsonl"):
        return "file.jsonl"
    if suffix.endswith(".csv"):
        return "file.csv"
    if suffix.endswith(".pickle") or suffix.endswith(".pkl"):
        return "file.pickle"
    raise OutputResolutionError(
        f"Unsupported output file extension for {path!s}. "
        "Expected one of: .jsonl, .jsonl.gz, .csv, .pickle"
    )


def _stdout_writer_for(fmt: str) -> str:
    match fmt:
        case "json-lines" | "jsonl" | "json":
            return "stdout.jsonl"
        case "print":
            return "stdout.print"
        case _:
            raise OutputResolutionError(
                f"Unsupported stdout format '{fmt}'. "
                "Expected 'print' or 'json-lines'."
            )


def resolve_output_target(
    *,
    cli_output: str | None,
    config_output: OutputConfig | None,
    default: str = "print",
    base_path: Path | None = None,
) -> OutputTarget:
    """
    Resolve the effective output target using CLI override, run config, or default.
    The CLI accepts 'print', 'stream', or a filesystem path.
    Run config accepts structured OutputConfig.
    """

    base_path = base_path or Path.cwd()

    if cli_output:
        choice = cli_output.strip()
        if choice.lower() in {"print", "stdout"}:
            return OutputTarget(writer_kind="stdout.print", destination=None)
        if choice.lower() in {"stream", "jsonl", "stdout.jsonl"}:
            return OutputTarget(writer_kind="stdout.jsonl", destination=None)
        dest_path = Path(choice)
        if not dest_path.is_absolute():
            dest_path = (base_path / dest_path).resolve()
        return OutputTarget(
            writer_kind=_path_writer_for(dest_path),
            destination=dest_path,
        )

    if config_output:
        if config_output.transport == "stdout":
            writer_kind = _stdout_writer_for(config_output.format)
            return OutputTarget(
                writer_kind=writer_kind,
                destination=None,
            )

        # filesystem target
        assert config_output.path is not None  # guarded by model validator
        dest_path = config_output.path
        if not dest_path.is_absolute():
            dest_path = (base_path / dest_path).resolve()
        match config_output.format:
            case "json-lines":
                writer_kind = "file.jsonl"
            case "csv":
                writer_kind = "file.csv"
            case "json":
                writer_kind = "file.jsonl"
            case "pickle":
                writer_kind = "file.pickle"
            case _:
                writer_kind = _path_writer_for(dest_path)
        return OutputTarget(
            writer_kind=writer_kind,
            destination=dest_path,
        )

    # fallback default
    if default == "print":
        return OutputTarget(writer_kind="stdout.print", destination=None)
    if default in {"stream", "jsonl"}:
        return OutputTarget(writer_kind="stdout.jsonl", destination=None)

    dest_path = Path(default)
    if not dest_path.is_absolute():
        dest_path = (base_path / dest_path).resolve()
    return OutputTarget(
        writer_kind=_path_writer_for(dest_path),
        destination=dest_path,
    )
