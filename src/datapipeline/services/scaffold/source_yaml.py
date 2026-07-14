from pathlib import Path

from datapipeline.services.paths import pkg_root
from datapipeline.services.project_paths import (
    ensure_project_scaffold,
    resolve_project_yaml_path,
)
from datapipeline.services.project import load_project
from datapipeline.services.scaffold.templates import render
from datapipeline.services.constants import (
    DEFAULT_IO_LOADER_EP,
    DEFAULT_SYNTHETIC_LOADER_EP,
)
from datapipeline.services.scaffold.utils import status


def _loader_args(transport: str, fmt: str | None) -> dict[str, object]:
    if transport == "fs":
        args: dict[str, object] = {
            "transport": "fs",
            "format": fmt or "<FORMAT (csv|json|jsonl|pickle)>",
            "path": "<PATH OR GLOB>",
            "encoding": "utf-8",
        }
        if fmt == "csv":
            args["delimiter"] = ","
        return args
    if transport == "http":
        args = {
            "transport": "http",
            "format": fmt or "<FORMAT (json|jsonl|csv)>",
            "url": "<https://api.example.com/data.json>",
            "headers": {},
            "params": {},
            "encoding": "utf-8",
        }
        if fmt == "csv":
            args["delimiter"] = ","
        return args
    if transport == "synthetic":
        return {"start": "<ISO8601>", "end": "<ISO8601>", "frequency": "1h"}
    return {}


def validate_source_id(source_id: str) -> None:
    parts = source_id.split(".")
    if len(parts) < 2 or any(
        not part
        or any(not (character.isalnum() or character in "_-") for character in part)
        for part in parts
    ):
        raise ValueError(
            "source_id must contain at least two dot-separated segments using only "
            "letters, numbers, underscores, or hyphens"
        )


def create_source_yaml(
    *,
    source_id: str,
    loader_ep: str,
    loader_args: dict[str, object],
    parser_ep: str,
    parser_args: dict[str, object] | None = None,
    root: Path | None,
    project_yaml: Path | None = None,
) -> None:
    root_dir, _, _ = pkg_root(root)
    validate_source_id(source_id)
    parser_args = parser_args or {}

    proj_yaml = (
        project_yaml.resolve()
        if project_yaml is not None
        else resolve_project_yaml_path(root_dir)
    )
    ensure_project_scaffold(proj_yaml)
    sources_dir = load_project(proj_yaml).source_dirs[0]
    sources_dir.mkdir(parents=True, exist_ok=True)

    src_cfg_path = sources_dir / f"{source_id}.yaml"
    if src_cfg_path.exists():
        status("skip", f"Source YAML already exists: {src_cfg_path.resolve()}")
        return

    src_cfg_path.write_text(
        render(
            "source.yaml.j2",
            id=source_id,
            parser_ep=parser_ep,
            parser_args=parser_args,
            loader_ep=loader_ep,
            loader_args=loader_args,
            default_io_loader_ep=DEFAULT_IO_LOADER_EP,
        ),
        encoding="utf-8",
    )
    status("new", str(src_cfg_path.resolve()))


def default_loader_config(
    transport: str,
    fmt: str | None,
) -> tuple[str, dict[str, object]]:
    if transport in {"fs", "http"}:
        return DEFAULT_IO_LOADER_EP, _loader_args(transport, fmt)
    if transport == "synthetic":
        return DEFAULT_SYNTHETIC_LOADER_EP, _loader_args(transport, fmt)
    return DEFAULT_IO_LOADER_EP, {}
