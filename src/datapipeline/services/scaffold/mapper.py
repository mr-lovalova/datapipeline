from pathlib import Path
from typing import Optional

from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    ep_key_from_name,
    info,
    status,
    to_snake,
    validate_identifier,
    write_if_missing,
)
from datapipeline.services.scaffold.layout import (
    DIR_MAPPERS,
    TPL_MAPPER_COMPOSED,
    TPL_MAPPER_INGEST,
    domain_record_class,
    entrypoint_target,
    pyproject_path,
)
from datapipeline.services.entrypoints import inject_ep
from datapipeline.services.constants import MAPPERS_GROUP


def create_mapper(
    *,
    name: str,
    dto_class: str | None = None,
    dto_module: str | None = None,
    input_class: str | None = None,
    input_module: str | None = None,
    domain: str,
    root: Optional[Path],
) -> str:
    validate_identifier(name, "Mapper name")

    root_dir, pkg_name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    package_name = base.name

    mappers_dir = ensure_pkg_dir(base, DIR_MAPPERS)
    module_name = to_snake(name)
    path = mappers_dir / f"{module_name}.py"

    domain_module = f"{package_name}.domains.{domain}.model"
    domain_record = domain_record_class(domain)

    resolved_class = input_class or dto_class
    resolved_module = input_module or dto_module
    if not resolved_class or not resolved_module:
        raise ValueError("Mapper input class/module is required")

    write_if_missing(
        path,
        render(
            TPL_MAPPER_INGEST,
            FUNCTION_NAME=module_name,
            INPUT_CLASS=resolved_class,
            INPUT_IMPORT=resolved_module,
            DOMAIN_MODULE=domain_module,
            DOMAIN_RECORD=domain_record,
        ),
        label="Mapper",
    )

    ep_key = ep_key_from_name(name)
    pyproject = pyproject_path(root_dir)
    try:
        toml_text = pyproject.read_text()
        updated = inject_ep(
            toml_text,
            MAPPERS_GROUP,
            ep_key,
            entrypoint_target(package_name, "mappers", module_name, module_name),
        )
        if updated != toml_text:
            pyproject.write_text(updated)
            status("ok", f"Registered mapper entry point '{ep_key}'.")
        else:
            status("skip", f"Mapper entry point already registered: '{ep_key}'.")
    except FileNotFoundError:
        info("pyproject.toml not found; skipping entry point registration")
    return ep_key


def create_composed_mapper(
    *,
    domain: str,
    stream_id: str,
    root: Optional[Path],
    mapper_path: str | None = None,
) -> str:
    root_dir, name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, name)
    map_pkg_dir = ensure_pkg_dir(base, DIR_MAPPERS)
    mapper_file = map_pkg_dir / f"{domain}.py"
    if not mapper_file.exists():
        mapper_file.write_text(render(TPL_MAPPER_COMPOSED))
        status("new", str(mapper_file))

    ep_key = stream_id
    package_name = base.name
    default_target = f"{package_name}.mappers.{domain}:mapper"
    ep_target = mapper_path if (mapper_path and ":" in mapper_path) else default_target
    pyproj_path = pyproject_path(root_dir)
    try:
        toml_text = pyproj_path.read_text()
        updated = inject_ep(toml_text, MAPPERS_GROUP, ep_key, ep_target)
        if updated != toml_text:
            pyproj_path.write_text(updated)
            status("ok", f"Registered mapper entry point '{ep_key}' -> {ep_target}")
    except FileNotFoundError:
        info("pyproject.toml not found; skipping entry point registration")
    return ep_key
