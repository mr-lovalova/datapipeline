from pathlib import Path
from typing import Optional

from datapipeline.services.scaffold.templates import render
from datapipeline.services.scaffold.utils import (
    ensure_pkg_dir,
    to_snake,
    validate_identifier,
    write_if_missing,
)
from datapipeline.services.scaffold.layout import DIR_DOMAINS, TPL_DOMAIN_RECORD, domain_record_class

from ..paths import pkg_root, resolve_base_pkg_dir


def create_domain(*, domain: str, root: Optional[Path]) -> None:
    validate_identifier(domain, "Domain name")
    root_dir, name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, name)
    package_name = base.name
    pkg_dir = ensure_pkg_dir(base / DIR_DOMAINS, domain)
    parent = "TemporalRecord"
    write_if_missing(
        pkg_dir / "model.py",
        render(
            TPL_DOMAIN_RECORD,
            PACKAGE_NAME=package_name,
            DOMAIN=domain,
            CLASS_NAME=domain_record_class(domain),
            PARENT_CLASS=parent,
            time_aware=True,
        ),
        label="Domain",
    )
