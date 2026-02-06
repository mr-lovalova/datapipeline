from pathlib import Path

from datapipeline.services.scaffold.loader import create_loader
from datapipeline.services.scaffold.utils import choose_name, status


def handle(name: str | None, *, plugin_root: Path | None = None) -> None:
    if not name:
        name = choose_name("Loader name", default="custom_loader")
    ep = create_loader(name=name, root=plugin_root)
    status("ok", f"Registered loader entry point '{ep}'.")
