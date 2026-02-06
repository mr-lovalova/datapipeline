from pathlib import Path
from typing import Optional

from datapipeline.services.paths import pkg_root
from datapipeline.services.project_paths import (
    sources_dir as resolve_sources_dir,
    ensure_project_scaffold,
    resolve_project_yaml_path,
)
from datapipeline.services.scaffold.templates import render
from datapipeline.services.constants import (
    DEFAULT_IO_LOADER_EP,
    DEFAULT_SYNTHETIC_LOADER_EP,
)
from datapipeline.services.scaffold.utils import status


def _loader_args(transport: str, fmt: Optional[str]) -> dict:
    if transport == "fs":
        args = {
            "transport": "fs",
            "format": fmt or "<FORMAT (csv|json|jsonl|pickle)>",
            "path": "<PATH OR GLOB>",
            "glob": False,
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


def create_source_yaml(
    *,
    provider: str,
    dataset: str,
    loader_ep: str,
    loader_args: dict,
    parser_ep: str,
    parser_args: dict | None = None,
    root: Optional[Path],
    project_yaml: Optional[Path] = None,
) -> None:
    root_dir, _, _ = pkg_root(root)
    alias = f"{provider}.{dataset}"
    parser_args = parser_args or {}

    proj_yaml = project_yaml.resolve() if project_yaml is not None else resolve_project_yaml_path(root_dir)
    ensure_project_scaffold(proj_yaml)
    sources_dir = resolve_sources_dir(proj_yaml).resolve()
    sources_dir.mkdir(parents=True, exist_ok=True)

    src_cfg_path = sources_dir / f"{alias}.yaml"
    if src_cfg_path.exists():
        status("skip", f"Source YAML already exists: {src_cfg_path.resolve()}")
        return

    src_cfg_path.write_text(
        render(
            "source.yaml.j2",
            id=alias,
            parser_ep=parser_ep,
            parser_args=parser_args,
            loader_ep=loader_ep,
            loader_args=loader_args,
            default_io_loader_ep=DEFAULT_IO_LOADER_EP,
        )
    )
    status("new", str(src_cfg_path.resolve()))


def default_loader_config(transport: str, fmt: Optional[str]) -> tuple[str, dict]:
    if transport in {"fs", "http"}:
        return DEFAULT_IO_LOADER_EP, _loader_args(transport, fmt)
    if transport == "synthetic":
        return DEFAULT_SYNTHETIC_LOADER_EP, _loader_args(transport, fmt)
    return DEFAULT_IO_LOADER_EP, {}
