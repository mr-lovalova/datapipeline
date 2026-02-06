from pathlib import Path

from datapipeline.services.scaffold.dto import create_dto
from datapipeline.services.scaffold.utils import status, prompt_required


def handle(name: str | None, *, plugin_root: Path | None = None) -> None:
    if not name:
        name = prompt_required("DTO class name")
    create_dto(name=name, root=plugin_root)
    status("ok", "DTO ready.")
