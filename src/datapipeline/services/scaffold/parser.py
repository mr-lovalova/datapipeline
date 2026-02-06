from pathlib import Path
from typing import Optional

from datapipeline.services.scaffold.templates import render
from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    ep_key_from_name,
    to_snake,
    validate_identifier,
    write_if_missing,
)
from datapipeline.services.scaffold.layout import DIR_PARSERS, TPL_PARSER
from datapipeline.services.scaffold.layout import entrypoint_target, pyproject_path
from datapipeline.services.entrypoints import inject_ep
from datapipeline.services.constants import PARSERS_GROUP


def create_parser(
    *,
    name: str,
    dto_class: str,
    dto_module: str,
    root: Optional[Path],
) -> str:
    validate_identifier(name, "Parser name")

    root_dir, pkg_name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    package_name = base.name

    parsers_dir = ensure_pkg_dir(base, DIR_PARSERS)
    module_name = to_snake(name)
    path = parsers_dir / f"{module_name}.py"

    write_if_missing(
        path,
        render(
            TPL_PARSER,
            CLASS_NAME=name,
            DTO_CLASS=dto_class,
            DTO_IMPORT=dto_module,
        ),
        label="Parser",
    )

    ep_key = ep_key_from_name(name)
    pyproject = pyproject_path(root_dir)
    toml = inject_ep(
        pyproject.read_text(),
        PARSERS_GROUP,
        ep_key,
        entrypoint_target(package_name, "parsers", module_name, name),
    )
    pyproject.write_text(toml)
    return ep_key
