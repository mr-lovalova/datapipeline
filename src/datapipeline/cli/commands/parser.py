from pathlib import Path

from datapipeline.services.scaffold.dto import create_dto
from datapipeline.services.scaffold.discovery import list_dtos
from datapipeline.services.scaffold.parser import create_parser
from datapipeline.services.scaffold.utils import (
    choose_existing_or_create,
    choose_name,
    error_exit,
    status,
)
from datapipeline.services.scaffold.layout import default_parser_name, LABEL_DTO_FOR_PARSER


def handle(
    name: str | None,
    *,
    plugin_root: Path | None = None,
    default_dto: str | None = None,
) -> str:
    dto_map = list_dtos(root=plugin_root)
    dto_class = choose_existing_or_create(
        label=LABEL_DTO_FOR_PARSER,
        existing=sorted(dto_map.keys()),
        create_label="Create new DTO",
        create_fn=create_dto,
        prompt_new="DTO class name",
        root=plugin_root,
        default_new=default_dto or (f"{name}DTO" if name else None),
    )
    dto_module = list_dtos(root=plugin_root).get(dto_class)
    if not dto_module:
        error_exit("Failed to resolve DTO module.")

    if not name:
        name = choose_name("Parser class name", default=default_parser_name(dto_class))

    ep = create_parser(
        name=name,
        dto_class=dto_class,
        dto_module=dto_module,
        root=plugin_root,
    )
    status("ok", f"Registered parser entry point '{ep}'.")
    return ep
