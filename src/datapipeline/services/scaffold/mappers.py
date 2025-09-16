from pathlib import Path
from typing import Optional
import re
from ..constants import MAPPERS_GROUP
from ..paths import pkg_root, resolve_base_pkg_dir
from ..entrypoints import inject_ep
from .templates import render, camel


def _slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def attach_source_to_domain(*, domain: str, provider: str, dataset: str, time_aware: bool, root: Optional[Path]) -> None:
    root_dir, name, pyproject = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, name)
    mappers_root = base / MAPPERS_GROUP
    prov = _slug(provider); ds = _slug(dataset); dom = _slug(domain)

    # Option B layout: mappers/{provider}/{dataset}/to_{domain}.py
    pkg_dir = mappers_root / provider / dataset
    pkg_dir.mkdir(parents=True, exist_ok=True)
    (mappers_root / "__init__.py").touch(exist_ok=True)
    (mappers_root / provider / "__init__.py").touch(exist_ok=True)
    (mappers_root / provider / dataset / "__init__.py").touch(exist_ok=True)

    module_name = f"to_{dom}"
    path = pkg_dir / f"{module_name}.py"
    if not path.exists():
        function_name = "map"
        path.write_text(render(
            "mapper.py.j2",
            PACKAGE_NAME=name,
            ORIGIN=provider,
            DATASET=dataset,
            TARGET_DOMAIN=dom,
            FUNCTION_NAME=function_name,
            DomainConfig=f"{camel(domain)}Config",
            DomainRecord=f"{camel(domain)}Record",
            OriginDTO=f"{camel(provider)}{camel(dataset)}DTO",
            time_aware=time_aware,
        ))
        print(f"✨ Created: {path}")

    # Register the mapper EP as domain.provider (fallback to domain.provider.dataset on collision handled elsewhere)
    ep_key = f"{dom}.{prov}"
    ep_target = f"{name}.mappers.{provider}.{dataset}.{module_name}:map"
    toml = (root_dir / "pyproject.toml").read_text()
    toml = inject_ep(toml, MAPPERS_GROUP, ep_key, ep_target)
    (root_dir / "pyproject.toml").write_text(toml)
