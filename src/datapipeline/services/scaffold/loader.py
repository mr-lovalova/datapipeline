from pathlib import Path

from datapipeline.plugins import LOADERS_EP
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
    DIR_LOADERS,
    TPL_LOADER_BASIC,
    ep_key_from_name,
    loader_class_name,
    to_snake,
)
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    is_python_identifier,
    write_new_file,
)


def validate_loader_creation(name: str, root: Path | None) -> None:
    if not is_python_identifier(name):
        raise ValueError("Loader name must be a valid Python identifier")
    root_dir, pkg_name, pyproject = pkg_root(root)
    entrypoint = ep_key_from_name(name)
    if entrypoint in read_entry_points(pyproject, LOADERS_EP):
        raise FileExistsError(f"Loader entry point '{entrypoint}' already exists")
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    path = base / DIR_LOADERS / f"{to_snake(name)}.py"
    if path.exists():
        raise FileExistsError(f"{path} already exists")


def create_loader(
    name: str,
    root: Path | None,
) -> str:
    validate_loader_creation(name, root)
    root_dir, pkg_name, pyproject = pkg_root(root)
    base = ensure_base_pkg_dir(root_dir, pkg_name)
    package_name = base.name

    loaders_dir = ensure_pkg_dir(base, DIR_LOADERS)
    module_name = to_snake(name)
    path = loaders_dir / f"{module_name}.py"

    class_name = loader_class_name(name)

    write_new_file(
        path,
        render(
            TPL_LOADER_BASIC,
            CLASS_NAME=class_name,
        ),
    )

    ep_key = ep_key_from_name(name)
    register_entry_point(
        pyproject,
        LOADERS_EP,
        ep_key,
        f"{package_name}.loaders.{module_name}:{class_name}",
    )
    return ep_key
