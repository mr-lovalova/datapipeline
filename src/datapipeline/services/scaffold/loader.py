from pathlib import Path
from typing import Optional

from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    ep_key_from_name,
    to_snake,
    validate_identifier,
    write_if_missing,
)
from datapipeline.services.scaffold.layout import (
    DIR_LOADERS,
    entrypoint_target,
    loader_class_name,
    loader_template_name,
    pyproject_path,
)
from datapipeline.services.entrypoints import inject_ep
from datapipeline.services.constants import LOADERS_GROUP


def create_loader(
    *,
    name: str,
    root: Optional[Path],
    template: str = "basic",
) -> str:
    validate_identifier(name, "Loader name")

    root_dir, pkg_name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    package_name = base.name

    loaders_dir = ensure_pkg_dir(base, DIR_LOADERS)
    module_name = to_snake(name)
    path = loaders_dir / f"{module_name}.py"

    class_name = loader_class_name(name)
    template_name = loader_template_name(template)

    write_if_missing(
        path,
        render(
            template_name,
            CLASS_NAME=class_name,
        ),
        label="Loader",
    )

    ep_key = ep_key_from_name(name)
    pyproject = pyproject_path(root_dir)
    toml = inject_ep(
        pyproject.read_text(),
        LOADERS_GROUP,
        ep_key,
        entrypoint_target(package_name, "loaders", module_name, class_name),
    )
    pyproject.write_text(toml)
    return ep_key
