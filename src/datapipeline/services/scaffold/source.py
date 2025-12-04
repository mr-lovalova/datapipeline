from ..constants import LOADERS_GROUP, PARSERS_GROUP
from pathlib import Path
from typing import Optional

from datapipeline.services.scaffold.templates import camel, render

from ..constants import DEFAULT_IO_LOADER_EP
from ..entrypoints import inject_ep
from ..paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.project_paths import (
    sources_dir as resolve_sources_dir,
    ensure_project_scaffold,
    resolve_project_yaml_path,
)


def _class_prefix(provider: str, dataset: str) -> str:
    """Single place to define class-prefix naming."""
    return f"{camel(provider)}{camel(dataset)}"


def _source_alias(provider: str, dataset: str) -> str:
    return f"{provider}.{dataset}"


def _write_if_missing(path: Path, text: str) -> None:
    """Write file only if it does not exist; echo a friendly message."""
    if not path.exists():
        path.write_text(text)
        print(f"[new] {path}")


def _render_loader_stub(transport: str, loader_class: str,
                        *, fmt: Optional[str]) -> str | None:
    """Render loader stub from Jinja templates for supported transports."""
    if transport == "synthetic":
        return render("loader_synthetic.py.j2", CLASS_NAME=loader_class)
    return None


def _update_ep(toml_text: str, provider: str, dataset: str, pkg_name: str,
               transport: str, parser_class: str, loader_class: str) -> tuple[str, str]:
    """
    Inject parser EP (always). Returns (updated_toml, ep_key).
    """
    ep_key = f"{provider}.{dataset}"
    toml_text = inject_ep(
        toml_text, PARSERS_GROUP, ep_key,
        f"{pkg_name}.sources.{provider}.{dataset}.parser:{parser_class}"
    )
    if transport in {"synthetic"}:
        toml_text = inject_ep(
            toml_text, LOADERS_GROUP, ep_key,
            f"{pkg_name}.sources.{provider}.{dataset}.loader:{loader_class}"
        )
    return toml_text, ep_key


def _loader_ep_and_args(transport: str, fmt: Optional[str], ep_key: Optional[str]) -> tuple[str, dict]:
    """Return (loader EP name, default args) for the YAML snippet."""
    if transport == "fs":
        args = {
            "transport": "fs",
            "format": fmt or "<FORMAT (csv|json|json-lines|pickle)>",
            "path": "<PATH OR GLOB>",
            "glob": False,
            "encoding": "utf-8",
        }
        if fmt == "csv":
            args["delimiter"] = ","
        return DEFAULT_IO_LOADER_EP, args
    if transport == "synthetic":
        if ep_key is None:
            raise ValueError("synthetic transport requires scaffolding a loader entrypoint")
        return ep_key, {"start": "<ISO8601>", "end": "<ISO8601>", "frequency": "1h"}
    if transport == "http":
        args = {
            "transport": "http",
            "format": fmt or "<FORMAT (json|json-lines|csv)>",
            "url": "<https://api.example.com/data.json>",
            "headers": {},
            "params": {},
            "encoding": "utf-8",
        }
        if fmt == "csv":
            args["delimiter"] = ","
        return DEFAULT_IO_LOADER_EP, args
    if ep_key is None:
        raise ValueError(f"unsupported transport '{transport}' for identity scaffold")
    return ep_key, {}


def create_source(
    *,
    provider: str,
    dataset: str,
    transport: str,
    format: Optional[str],
    root: Optional[Path],
    identity: bool = False,
) -> None:
    root_dir, name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, name)
    package_name = base.name

    alias = _source_alias(provider, dataset)
    parser_ep: str
    parser_args: dict
    ep_key: Optional[str] = None

    if identity:
        if transport == "synthetic":
            raise ValueError(
                "identity parser scaffold is not supported for synthetic sources; "
                "generate the standard parser instead."
            )
        parser_ep = "identity"
        parser_args = {}
    else:
        src_pkg_dir = base / "sources" / provider / dataset
        src_pkg_dir.mkdir(parents=True, exist_ok=True)
        (src_pkg_dir / "__init__.py").touch(exist_ok=True)

        class_prefix = _class_prefix(provider, dataset)
        dto_class = f"{class_prefix}DTO"
        parser_class = f"{class_prefix}Parser"
        loader_class = f"{class_prefix}DataLoader"

        # DTO
        dto_path = src_pkg_dir / "dto.py"
        _write_if_missing(dto_path, render(
            "dto.py.j2",
            PACKAGE_NAME=package_name, ORIGIN=provider, DOMAIN=dataset,
            CLASS_NAME=dto_class, time_aware=True
        ))

        # Parser
        parser_path = src_pkg_dir / "parser.py"
        _write_if_missing(parser_path, render(
            "parser.py.j2",
            PACKAGE_NAME=package_name, ORIGIN=provider, DOMAIN=dataset,
            CLASS_NAME=parser_class, DTO_CLASS=dto_class, time_aware=True
        ))

        # Optional loader stub: synthetic (http uses core IO loader by default)
        if transport in {"synthetic"}:
            loader_path = src_pkg_dir / "loader.py"
            stub = _render_loader_stub(transport, loader_class, fmt=format)
            if stub is not None:
                _write_if_missing(loader_path, stub)

        toml_path = root_dir / "pyproject.toml"
        toml_text, ep_key = _update_ep(
            toml_path.read_text(),
            provider,
            dataset,
            package_name,
            transport,
            parser_class,
            loader_class,
        )
        toml_path.write_text(toml_text)

        parser_ep = ep_key
        parser_args = {}

    loader_ep, loader_args = _loader_ep_and_args(transport, format, ep_key)

    # Resolve sources directory from a single dataset-scoped project config.
    # If not present or invalid, let the exception bubble up to prompt the user
    # to provide a valid project path.
    proj_yaml = resolve_project_yaml_path(root_dir)
    # Best-effort: create a minimal project scaffold if missing
    ensure_project_scaffold(proj_yaml)
    sources_dir = resolve_sources_dir(proj_yaml).resolve()
    sources_dir.mkdir(parents=True, exist_ok=True)
    src_cfg_path = sources_dir / f"{alias}.yaml"
    if not src_cfg_path.exists():
        src_cfg_path.write_text(render(
            "source.yaml.j2",
            id=alias,
            parser_ep=parser_ep,
            parser_args=parser_args,
            loader_ep=loader_ep,
            loader_args=loader_args,
            default_io_loader_ep=DEFAULT_IO_LOADER_EP,
        ))
        print(f"[new] {src_cfg_path.resolve()}")
    elif identity:
        print(f"[info] Source YAML already exists; skipped identity scaffold at {src_cfg_path.resolve()}")
