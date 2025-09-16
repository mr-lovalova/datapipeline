from pathlib import Path
from typing import Optional
from datapipeline.services.scaffold.templates import render
from ..constants import FILTERS_GROUP
from ..entrypoints import inject_ep
from ..paths import pkg_root, resolve_base_pkg_dir


def create_filter(*, name: str, root: Optional[Path]) -> None:
    root_dir, pkg_name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, pkg_name)
    filters_dir = base / FILTERS_GROUP
    filters_dir.mkdir(parents=True, exist_ok=True)
    (filters_dir / "__init__.py").touch(exist_ok=True)

    # Create filter function module
    module_name = name
    path = filters_dir / f"{module_name}.py"
    if not path.exists():
        path.write_text(render("filter.py.j2", FUNCTION_NAME=name))
        print(f"✨ Created: {path}")

    # Register entry point under datapipeline.filters
    toml_path = root_dir / "pyproject.toml"
    toml = inject_ep(
        toml_path.read_text(),
        FILTERS_GROUP,
        name,
        f"{pkg_name}.filters.{module_name}:{name}",
    )
    toml_path.write_text(toml)

