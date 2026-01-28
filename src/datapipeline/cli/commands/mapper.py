from pathlib import Path

from datapipeline.services.scaffold.discovery import list_domains, list_dtos
from datapipeline.services.scaffold.dto import create_dto
from datapipeline.services.scaffold.domain import create_domain
from datapipeline.services.scaffold.mapper import create_mapper
from datapipeline.services.scaffold.utils import (
    choose_existing_or_create,
    choose_name,
    error_exit,
    info,
    status,
    pick_from_menu,
    pick_from_list,
)
from datapipeline.services.scaffold.layout import (
    default_mapper_name,
    LABEL_DTO_FOR_MAPPER,
    LABEL_DOMAIN_TO_MAP,
    LABEL_MAPPER_INPUT,
    default_mapper_name_for_identity,
)


def handle(name: str | None, *, plugin_root: Path | None = None) -> str:
    input_class = None
    input_module = None

    input_choice = pick_from_menu(
        f"{LABEL_MAPPER_INPUT}:",
        [
            ("dto", "DTO (default)"),
            ("identity", "Any"),
        ],
    )
    info("Mapper output (select domain):")

    dto_map = list_dtos(root=plugin_root)
    if input_choice == "dto":
        dto_class = choose_existing_or_create(
            label=LABEL_DTO_FOR_MAPPER,
            existing=sorted(dto_map.keys()),
            create_label="Create new DTO",
            create_fn=create_dto,
            prompt_new="DTO class name",
            root=plugin_root,
        )
        dto_module = list_dtos(root=plugin_root).get(dto_class)
        if not dto_module:
            error_exit("Failed to resolve DTO module.")
        input_class = dto_class
        input_module = dto_module
    else:
        input_module = "typing"
        input_class = "Any"

    domains = list_domains(root=plugin_root)
    domain = choose_existing_or_create(
        label=LABEL_DOMAIN_TO_MAP,
        existing=domains,
        create_label="Create new domain",
        create_fn=lambda name, root: create_domain(domain=name, root=root),
        prompt_new="Domain name",
        root=plugin_root,
    )

    if not name:
        if input_choice == "identity":
            name = choose_name(
                "Mapper name", default=default_mapper_name_for_identity(domain))
        else:
            name = choose_name(
                "Mapper name", default=default_mapper_name(input_module, domain))

    ep = create_mapper(
        name=name,
        input_class=input_class,
        input_module=input_module,
        domain=domain,
        root=plugin_root,
    )
    return ep
