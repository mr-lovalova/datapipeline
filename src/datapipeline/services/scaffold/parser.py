from pathlib import Path

from datapipeline.plugins import PARSERS_EP
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
    DIR_PARSERS,
    TPL_PARSER,
    ep_key_from_name,
    to_snake,
)
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    is_python_identifier,
    write_new_file,
)


def validate_parser_creation(name: str, root: Path | None) -> None:
    if not is_python_identifier(name):
        raise ValueError("Parser name must be a valid Python identifier")
    root_dir, pkg_name, pyproject = pkg_root(root)
    entrypoint = ep_key_from_name(name)
    if entrypoint in read_entry_points(pyproject, PARSERS_EP):
        raise FileExistsError(f"Parser entry point '{entrypoint}' already exists")
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    path = base / DIR_PARSERS / f"{to_snake(name)}.py"
    if path.exists():
        raise FileExistsError(f"{path} already exists")


def create_parser(
    *,
    name: str,
    dto_class: str,
    dto_module: str,
    root: Path | None,
) -> str:
    validate_parser_creation(name, root)
    root_dir, pkg_name, pyproject = pkg_root(root)
    base = ensure_base_pkg_dir(root_dir, pkg_name)
    package_name = base.name

    parsers_dir = ensure_pkg_dir(base, DIR_PARSERS)
    module_name = to_snake(name)
    path = parsers_dir / f"{module_name}.py"

    write_new_file(
        path,
        render(
            TPL_PARSER,
            CLASS_NAME=name,
            DTO_CLASS=dto_class,
            DTO_IMPORT=dto_module,
        ),
    )

    ep_key = ep_key_from_name(name)
    register_entry_point(
        pyproject,
        PARSERS_EP,
        ep_key,
        f"{package_name}.parsers.{module_name}:{name}",
    )
    return ep_key
