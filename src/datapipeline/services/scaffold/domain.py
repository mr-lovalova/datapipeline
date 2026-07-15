from pathlib import Path

from datapipeline.services.paths import (
    ensure_base_pkg_dir,
    pkg_root,
    resolve_base_pkg_dir,
)
from datapipeline.services.scaffold.layout import (
    DIR_DOMAINS,
    TPL_DOMAIN_RECORD,
    domain_record_class,
)
from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    is_python_identifier,
    write_new_file,
)


def validate_domain_creation(domain: str, root: Path | None) -> None:
    if not is_python_identifier(domain):
        raise ValueError("Domain name must be a valid Python identifier")
    root_dir, name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, name)
    path = base / DIR_DOMAINS / domain / "model.py"
    if path.exists():
        raise FileExistsError(f"{path} already exists")


def create_domain(domain: str, root: Path | None) -> Path:
    validate_domain_creation(domain, root)
    root_dir, name, _ = pkg_root(root)
    base = ensure_base_pkg_dir(root_dir, name)
    package_name = base.name
    pkg_dir = ensure_pkg_dir(base / DIR_DOMAINS, domain)
    parent = "TemporalRecord"
    path = pkg_dir / "model.py"
    write_new_file(
        path,
        render(
            TPL_DOMAIN_RECORD,
            PACKAGE_NAME=package_name,
            DOMAIN=domain,
            CLASS_NAME=domain_record_class(domain),
            PARENT_CLASS=parent,
            time_aware=True,
        ),
    )
    return path
