from pathlib import Path
from typing import Optional
from datapipeline.services.scaffold.templates import render
from ..paths import pkg_root, resolve_base_pkg_dir


def create_domain(*, domain: str, time_aware: bool, root: Optional[Path]) -> None:
    root_dir, name, _ = pkg_root(root)
    base = resolve_base_pkg_dir(root_dir, name)
    pkg_dir = base / "domains" / domain
    pkg_dir.mkdir(parents=True, exist_ok=True)
    (pkg_dir / "__init__.py").touch(exist_ok=True)

    def write_missing(path: Path, tpl: str, **ctx):
        if not path.exists():
            path.write_text(render(tpl, **ctx))
            print(f"✨ Created: {path}")

    cls_ = "".join(w.capitalize() for w in domain.split("_"))
    parent = "TimeFeatureRecord" if time_aware else "Record"
    write_missing(pkg_dir / "model.py", "record.py.j2",
                  PACKAGE_NAME=name, DOMAIN=domain, CLASS_NAME=f"{cls_}Record",
                  PARENT_CLASS=parent, time_aware=time_aware)
