import re
from pathlib import Path

from datapipeline.services.project import load_project
from datapipeline.services.scaffold.locking import ScaffoldLock, acquire_scaffold_lock
from datapipeline.services.scaffold.paths import (
    default_project_yaml_path,
    ensure_project_scaffold,
    pkg_root,
)
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import write_new_file
from datapipeline.services.streams.loader import declared_source_ids

DEFAULT_IO_LOADER_EP = "core.io"
DEFAULT_SYNTHETIC_LOADER_EP = "core.synthetic.ticks"
DEFAULT_TEMPORAL_RECORD_PARSER_EP = "core.temporal_record"


def _loader_args(transport: str, fmt: str | None) -> dict[str, object]:
    if fmt not in {None, "csv", "json", "jsonl"}:
        raise ValueError(f"Unsupported source format: {fmt!r}")
    if transport == "fs":
        args: dict[str, object] = {
            "transport": "fs",
            "format": fmt or "<FORMAT (csv|json|jsonl)>",
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
    raise ValueError(f"Unsupported source transport: {transport!r}")


def validate_source_id(source_id: str) -> None:
    if (
        "." not in source_id
        or re.fullmatch(
            r"[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*",
            source_id,
        )
        is None
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
    scaffold_lock: ScaffoldLock | None = None,
) -> Path:
    root_dir, _, _ = pkg_root(root)
    proj_yaml = (
        project_yaml.resolve()
        if project_yaml is not None
        else default_project_yaml_path(root_dir)
    )
    with acquire_scaffold_lock(proj_yaml.parent, scaffold_lock) as project_lock:
        validate_source_id(source_id)
        parser_args = parser_args or {}
        if proj_yaml.exists():
            project = load_project(proj_yaml)
            existing_path = project.source_dirs[0] / f"{source_id}.yaml"
            if existing_path.exists():
                raise FileExistsError(f"{existing_path} already exists")
        project = ensure_project_scaffold(proj_yaml, project_lock)
        if source_id in declared_source_ids(project):
            raise FileExistsError(f"Source id '{source_id}' already exists")
        src_cfg_path = project.source_dirs[0] / f"{source_id}.yaml"
        write_new_file(
            src_cfg_path,
            render(
                "source.yaml.j2",
                id=source_id,
                parser_ep=parser_ep,
                parser_args=parser_args,
                loader_ep=loader_ep,
                loader_args=loader_args,
                default_io_loader_ep=DEFAULT_IO_LOADER_EP,
            ),
        )
        return src_cfg_path.resolve()


def default_loader_config(
    transport: str,
    fmt: str | None,
) -> tuple[str, dict[str, object]]:
    if transport in {"fs", "http"}:
        return DEFAULT_IO_LOADER_EP, _loader_args(transport, fmt)
    if transport == "synthetic":
        return DEFAULT_SYNTHETIC_LOADER_EP, _loader_args(transport, fmt)
    raise ValueError(f"Unsupported source transport: {transport!r}")
