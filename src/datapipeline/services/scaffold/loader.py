from pathlib import Path
from typing import Optional

from datapipeline.plugins import LOADERS_EP
from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
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
    validate_identifier,
    write_if_missing,
)


def create_loader(
    *,
    name: str,
    root: Optional[Path],
) -> str:
    validate_identifier(name, "Loader name")

    root_dir, pkg_name, pyproject = pkg_root(root)
    read_entry_points(pyproject, LOADERS_EP)
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    package_name = base.name

    loaders_dir = ensure_pkg_dir(base, DIR_LOADERS)
    module_name = to_snake(name)
    path = loaders_dir / f"{module_name}.py"

    class_name = loader_class_name(name)

    write_if_missing(
        path,
        render(
            TPL_LOADER_BASIC,
            CLASS_NAME=class_name,
        ),
        label="Loader",
    )

    ep_key = ep_key_from_name(name)
    register_entry_point(
        pyproject,
        LOADERS_EP,
        ep_key,
        f"{package_name}.loaders.{module_name}:{class_name}",
    )
    return ep_key
