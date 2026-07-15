import logging
from pathlib import Path

from datapipeline.cli.prompts import (
    choose_domain,
    choose_dto,
    choose_name,
    pick_from_menu,
)
from datapipeline.services.paths import pkg_root
from datapipeline.services.scaffold.discovery import list_domains, list_dtos
from datapipeline.services.scaffold.dto import create_dto, validate_dto_creation
from datapipeline.services.scaffold.domain import (
    create_domain,
    validate_domain_creation,
)
from datapipeline.services.scaffold.layout import (
    default_mapper_name,
    default_mapper_name_for_identity,
    dto_module_path,
)
from datapipeline.services.scaffold.mapper import (
    create_mapper,
    validate_mapper_creation,
)

logger = logging.getLogger(__name__)


def handle(name: str | None, *, plugin_root: Path | None = None) -> str:
    input_choice = pick_from_menu(
        "Mapper input:",
        [
            ("dto", "DTO (default)"),
            ("identity", "Any"),
        ],
    )

    dto_to_create = None
    if input_choice == "dto":
        dto_map = list_dtos(root=plugin_root)
        dto_class, should_create_dto = choose_dto(sorted(dto_map))
        if should_create_dto:
            _, package_name, _ = pkg_root(plugin_root)
            dto_module = dto_module_path(package_name, dto_class)
            dto_to_create = dto_class
        else:
            dto_module = dto_map[dto_class]
        input_class = dto_class
        input_module = dto_module
    elif input_choice == "identity":
        input_module = "typing"
        input_class = "Any"
    else:
        raise ValueError(f"Unknown mapper input choice: {input_choice}")

    logger.info("Mapper output:")
    domain, should_create_domain = choose_domain(list_domains(root=plugin_root))

    if not name:
        if input_choice == "identity":
            name = choose_name(
                "Mapper name", default=default_mapper_name_for_identity(domain)
            )
        else:
            name = choose_name(
                "Mapper name", default=default_mapper_name(input_module, domain)
            )

    try:
        if dto_to_create is not None:
            validate_dto_creation(dto_to_create, plugin_root)
        if should_create_domain:
            validate_domain_creation(domain, plugin_root)
        validate_mapper_creation(name, plugin_root)
        if dto_to_create is not None:
            create_dto(dto_to_create, plugin_root)
        if should_create_domain:
            create_domain(domain, plugin_root)
        entrypoint = create_mapper(
            name=name,
            input_class=input_class,
            input_module=input_module,
            domain=domain,
            root=plugin_root,
        )
    except (FileExistsError, ValueError) as exc:
        raise SystemExit(str(exc)) from None
    logger.info("Mapper entry point: %s", entrypoint)
    return entrypoint
