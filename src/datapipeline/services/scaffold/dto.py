from pathlib import Path

from datapipeline.services.paths import (
    ensure_base_pkg_dir,
    pkg_root,
    resolve_base_pkg_dir,
)
from datapipeline.services.scaffold.layout import DIR_DTOS, TPL_DTO, to_snake
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    is_python_identifier,
    write_new_file,
)


def validate_dto_creation(name: str, root: Path | None) -> None:
    if not is_python_identifier(name):
        raise ValueError("DTO name must be a valid Python identifier")
    root_dir, pkg_name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    path = base / DIR_DTOS / f"{to_snake(name)}.py"
    if path.exists():
        raise FileExistsError(f"{path} already exists")


def create_dto(name: str, root: Path | None) -> Path:
    validate_dto_creation(name, root)
    root_dir, pkg_name, _ = pkg_root(root)
    base = ensure_base_pkg_dir(root_dir, pkg_name)
    dtos_dir = ensure_pkg_dir(base, DIR_DTOS)
    module_name = to_snake(name)
    path = dtos_dir / f"{module_name}.py"
    write_new_file(
        path,
        render(
            TPL_DTO,
            CLASS_NAME=name,
            DOMAIN=name,
        ),
    )
    return path
