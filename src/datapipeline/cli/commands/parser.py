import logging
from pathlib import Path

from datapipeline.cli.prompts import choose_dto, choose_name
from datapipeline.services.paths import pkg_root
from datapipeline.services.scaffold.dto import create_dto, validate_dto_creation
from datapipeline.services.scaffold.discovery import list_dtos
from datapipeline.services.scaffold.layout import default_parser_name, dto_module_path
from datapipeline.services.scaffold.parser import (
    create_parser,
    validate_parser_creation,
)

logger = logging.getLogger(__name__)


def handle(
    name: str | None,
    *,
    plugin_root: Path | None = None,
) -> str:
    dto_map = list_dtos(root=plugin_root)
    dto_class, should_create_dto = choose_dto(
        sorted(dto_map),
        f"{name}DTO" if name else None,
    )

    _, package_name, _ = pkg_root(plugin_root)
    if should_create_dto:
        dto_module = dto_module_path(package_name, dto_class)
    else:
        dto_module = dto_map[dto_class]

    if not name:
        name = choose_name("Parser class name", default=default_parser_name(dto_class))

    try:
        if should_create_dto:
            validate_dto_creation(dto_class, plugin_root)
        validate_parser_creation(name, plugin_root)
        if should_create_dto:
            create_dto(dto_class, plugin_root)
        entrypoint = create_parser(
            name=name,
            dto_class=dto_class,
            dto_module=dto_module,
            root=plugin_root,
        )
    except (FileExistsError, ValueError) as exc:
        raise SystemExit(str(exc)) from None
    logger.info("Parser entry point: %s", entrypoint)
    return entrypoint
