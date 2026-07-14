from pathlib import Path

from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    info,
    status,
    validate_identifier,
    write_if_missing,
)
from datapipeline.services.scaffold.layout import (
    DIR_MAPPERS,
    TPL_MAPPER_SOURCE,
    domain_record_class,
    ep_key_from_name,
    entrypoint_target,
    pyproject_path,
    to_snake,
)
from datapipeline.services.entrypoints import inject_ep
from datapipeline.services.constants import MAPPERS_GROUP


def create_mapper(
    *,
    name: str,
    input_class: str,
    input_module: str,
    domain: str,
    root: Path | None,
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

    write_if_missing(
        path,
        render(
            TPL_MAPPER_SOURCE,
            FUNCTION_NAME=module_name,
            INPUT_CLASS=input_class,
            INPUT_IMPORT=input_module,
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
