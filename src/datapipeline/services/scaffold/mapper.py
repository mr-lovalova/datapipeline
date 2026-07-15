from pathlib import Path

from datapipeline.plugins import MAPPERS_EP
from datapipeline.services.paths import (
    ensure_base_pkg_dir,
    pkg_root,
    resolve_base_pkg_dir,
)
from datapipeline.services.scaffold.entrypoints import (
    read_entry_points,
    register_entry_point,
)
from datapipeline.services.scaffold.layout import (
    DIR_MAPPERS,
    TPL_MAPPER_SOURCE,
    domain_record_class,
    ep_key_from_name,
    to_snake,
)
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    is_python_identifier,
    write_new_file,
)


def validate_mapper_creation(name: str, root: Path | None) -> None:
    if not is_python_identifier(name):
        raise ValueError("Mapper name must be a valid Python identifier")
    root_dir, pkg_name, pyproject = pkg_root(root)
    entrypoint = ep_key_from_name(name)
    if entrypoint in read_entry_points(pyproject, MAPPERS_EP):
        raise FileExistsError(f"Mapper entry point '{entrypoint}' already exists")
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    path = base / DIR_MAPPERS / f"{to_snake(name)}.py"
    if path.exists():
        raise FileExistsError(f"{path} already exists")


def create_mapper(
    *,
    name: str,
    input_class: str,
    input_module: str,
    domain: str,
    root: Path | None,
) -> str:
    validate_mapper_creation(name, root)
    root_dir, pkg_name, pyproject = pkg_root(root)
    base = ensure_base_pkg_dir(root_dir, pkg_name)
    package_name = base.name

    mappers_dir = ensure_pkg_dir(base, DIR_MAPPERS)
    module_name = to_snake(name)
    path = mappers_dir / f"{module_name}.py"

    domain_module = f"{package_name}.domains.{domain}.model"
    domain_record = domain_record_class(domain)

    write_new_file(
        path,
        render(
            TPL_MAPPER_SOURCE,
            FUNCTION_NAME=module_name,
            INPUT_CLASS=input_class,
            INPUT_IMPORT=input_module,
            DOMAIN_MODULE=domain_module,
            DOMAIN_RECORD=domain_record,
        ),
    )

    ep_key = ep_key_from_name(name)
    register_entry_point(
        pyproject,
        MAPPERS_EP,
        ep_key,
        f"{package_name}.mappers.{module_name}:{module_name}",
    )
    return ep_key
