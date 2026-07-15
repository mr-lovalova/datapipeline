from pathlib import Path
from typing import Optional

from datapipeline.plugins import PARSERS_EP
from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.scaffold.entrypoints import (
    read_entry_points,
    register_entry_point,
)
from datapipeline.services.scaffold.layout import (
    DIR_PARSERS,
    TPL_PARSER,
    ep_key_from_name,
    to_snake,
)
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    validate_identifier,
    write_if_missing,
)


def create_parser(
    *,
    name: str,
    dto_class: str,
    dto_module: str,
    root: Optional[Path],
) -> str:
    validate_identifier(name, "Parser name")

    root_dir, pkg_name, pyproject = pkg_root(root)
    read_entry_points(pyproject, PARSERS_EP)
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
    register_entry_point(
        pyproject,
        PARSERS_EP,
        ep_key,
        f"{package_name}.parsers.{module_name}:{name}",
    )
    return ep_key
