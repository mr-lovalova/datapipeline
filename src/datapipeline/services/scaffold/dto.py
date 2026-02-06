from pathlib import Path
from typing import Optional

from datapipeline.services.scaffold.templates import render
from datapipeline.services.paths import pkg_root, resolve_base_pkg_dir
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    to_snake,
    validate_identifier,
    write_if_missing,
)
from datapipeline.services.scaffold.layout import DIR_DTOS, TPL_DTO


def create_dto(*, name: str, root: Optional[Path]) -> None:
    validate_identifier(name, "DTO name")

    root_dir, pkg_name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    dtos_dir = ensure_pkg_dir(base, DIR_DTOS)
    module_name = to_snake(name)
    path = dtos_dir / f"{module_name}.py"
    write_if_missing(
        path,
        render(
            TPL_DTO,
            CLASS_NAME=name,
            DOMAIN=name,
        ),
        label="DTO",
    )
