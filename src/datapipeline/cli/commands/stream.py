from dataclasses import dataclass
from pathlib import Path

from datapipeline.config.workspace import WorkspaceContext
from datapipeline.cli.workspace_utils import resolve_default_project_yaml
from datapipeline.config.options import SOURCE_TRANSPORTS, source_formats_for
from datapipeline.services.paths import pkg_root
from datapipeline.services.project_paths import resolve_project_yaml_path
from datapipeline.services.scaffold.discovery import (
    list_domains,
    list_mappers,
    list_parsers,
    list_sources,
    list_dtos,
)
from datapipeline.services.scaffold.source_yaml import default_loader_config
from datapipeline.services.scaffold.layout import (
    default_stream_id,
    dto_class_name,
    default_parser_name,
    default_mapper_name,
    dto_module_path,
    LABEL_DTO_FOR_PARSER,
    LABEL_DTO_FOR_MAPPER,
    LABEL_DOMAIN_TO_MAP,
    LABEL_MAPPER_INPUT,
    default_mapper_name_for_identity,
)
from datapipeline.services.scaffold.stream_plan import StreamPlan, ParserPlan, MapperPlan, execute_stream_plan
from datapipeline.services.scaffold.utils import (
    choose_name,
    error_exit,
    info,
    pick_from_list,
    pick_from_menu,
    prompt_required,
    choose_existing_or_create_name,
)


@dataclass
class StreamSelection:
    provider: str
    dataset: str
    source_id: str
    create_source: bool
    loader_ep: str | None
    loader_args: dict | None
    pchoice: str
    parser_create_dto: bool
    dto_class: str | None
    dto_module: str | None
    parser_name: str | None
    parser_ep: str | None
    domain: str
    create_domain: bool
    mchoice: str
    mapper_create_dto: bool
    mapper_input_class: str | None
    mapper_input_module: str | None
    mapper_name: str | None
    mapper_ep: str | None
    stream_id: str


def _parser_menu_options(parsers: dict[str, str]) -> list[tuple[str, str]]:
    base = [("create", "Create new parser (default)"), ("identity", "Identity parser")]
    if parsers:
        return [base[0], ("existing", "Select existing parser"), base[1]]
    return base


def _mapper_menu_options(mappers: dict[str, str]) -> list[tuple[str, str]]:
    base = [("create", "Create new mapper (default)"), ("identity", "Identity mapper")]
    if mappers:
        return [base[0], ("existing", "Select existing mapper"), base[1]]
    return base


def _build_parser_plan(
    *,
    choice: str,
    create_dto: bool,
    dto_class: str | None,
    dto_module: str | None,
    parser_name: str | None,
    parser_ep: str | None,
) -> ParserPlan:
    if choice == "create":
        return ParserPlan(
            create=True,
            create_dto=create_dto,
            dto_class=dto_class,
            dto_module=dto_module,
            parser_name=parser_name,
        )
    if choice == "existing":
        return ParserPlan(create=False, parser_ep=parser_ep)
    return ParserPlan(create=False, parser_ep="identity")


def _build_mapper_plan(
    *,
    choice: str,
    create_dto: bool,
    input_class: str | None,
    input_module: str | None,
    mapper_name: str | None,
    mapper_ep: str | None,
    domain: str,
) -> MapperPlan:
    if choice == "create":
        return MapperPlan(
            create=True,
            create_dto=create_dto,
            input_class=input_class,
            input_module=input_module,
            mapper_name=mapper_name,
            domain=domain,
        )
    if choice == "existing":
        return MapperPlan(create=False, mapper_ep=mapper_ep, domain=domain)
    return MapperPlan(create=False, mapper_ep="identity", domain=domain)


def _collect_stream_selection(
    *,
    plugin_root: Path | None,
    pkg_name: str,
    project_yaml: Path,
) -> StreamSelection:
    # Shared context
    provider = prompt_required("Provider name (e.g. nasa)")
    dataset = prompt_required("Dataset name (e.g. weather)")
    source_id = f"{provider}.{dataset}"

    create_source = False
    create_domain = False
    parser_create_dto = False
    mapper_create_dto = False

    dto_class = None
    dto_module = None
    mapper_input_class = None
    mapper_input_module = None
    loader_ep = None
    loader_args = None
    parser_ep = None
    mapper_ep = None
    parser_name = None
    mapper_name = None
    pchoice = "identity"
    mchoice = "identity"

    source_choice = pick_from_menu(
        "Source:",
        [
            ("create", "Create new source (default)"),
            ("existing", "Select existing source"),
        ],
    )

    if source_choice == "existing":
        sources = list_sources(project_yaml)
        if not sources:
            error_exit("No sources found. Create one first.")
        source_id = pick_from_list("Select source:", sources)
        parts = source_id.split(".", 1)
        provider = parts[0] if len(parts) == 2 else provider
        dataset = parts[1] if len(parts) == 2 else dataset
    else:
        source_id_default = f"{provider}.{dataset}"
        source_id = choose_name("Source id", default=source_id_default)
        create_source = True

        # Loader selection
        loader_ep = None
        loader_args = {}
        choice = pick_from_menu(
            "Loader:",
            [
                ("fs", "Built-in fs"),
                ("http", "Built-in http"),
                ("synthetic", "Built-in synthetic"),
                ("custom", "Custom loader"),
            ],
            allow_default=False,
        )
        if choice in SOURCE_TRANSPORTS:
            if choice in {"fs", "http"}:
                fmt = pick_from_menu(
                    "Format:",
                    [(name, name) for name in source_formats_for(choice)],
                    allow_default=False,
                )
            else:
                fmt = None
            loader_ep, loader_args = default_loader_config(choice, fmt)
        else:
            loader_ep = prompt_required("Loader entrypoint")

        # Parser selection
        parsers = list_parsers(root=plugin_root)
        pchoice = pick_from_menu("Parser:", _parser_menu_options(parsers))
        if pchoice == "existing":
            parser_ep = pick_from_menu(
                "Select parser entrypoint:",
                [(k, k) for k in sorted(parsers.keys())],
            )
        elif pchoice == "create":
            dto_default = dto_class_name(
                f"{provider}_{dataset}") if provider and dataset else None
            dto_class, parser_create_dto = choose_existing_or_create_name(
                label=LABEL_DTO_FOR_PARSER,
                existing=sorted(list_dtos(root=plugin_root).keys()),
                create_label="Create new DTO",
                prompt_new="DTO class name",
                default_new=dto_default,
            )
            parser_name = choose_name(
                "Parser class name",
                default=default_parser_name(dto_class),
            )
            dto_module = dto_module_path(pkg_name, dto_class)
        elif pchoice == "identity":
            parser_ep = "identity"
        else:
            parser_ep = "identity"

    domain, create_domain = choose_existing_or_create_name(
        label=LABEL_DOMAIN_TO_MAP,
        existing=list_domains(root=plugin_root),
        create_label="Create new domain",
        prompt_new="Domain name",
        default_new=dataset,
    )

    mappers = list_mappers(root=plugin_root)
    mchoice = pick_from_menu("Mapper:", _mapper_menu_options(mappers))
    if mchoice == "existing":
        mapper_ep = pick_from_menu(
            "Select mapper entrypoint:",
            [(k, k) for k in sorted(mappers.keys())],
        )
    elif mchoice == "create":
        input_choice = pick_from_menu(
            f"{LABEL_MAPPER_INPUT}:",
            [
                ("dto", "DTO (default)"),
                ("identity", "Any"),
            ],
        )
        info("Domain output: Domain record")
        if input_choice == "dto":
            if not dto_class:
                dto_class, mapper_create_dto = choose_existing_or_create_name(
                    label=LABEL_DTO_FOR_MAPPER,
                    existing=sorted(list_dtos(root=plugin_root).keys()),
                    create_label="Create new DTO",
                    prompt_new="DTO class name",
                    default_new=dto_class_name(f"{provider}_{dataset}"),
                )
            else:
                mapper_create_dto = False
            dto_module = dto_module_path(pkg_name, dto_class)
            mapper_input_class = dto_class
            mapper_input_module = dto_module
        else:
            mapper_input_module = "typing"
            mapper_input_class = "Any"
            mapper_create_dto = False
        if input_choice == "identity":
            mapper_name = choose_name(
                "Mapper name",
                default=default_mapper_name_for_identity(domain),
            )
        else:
            mapper_name = choose_name(
                "Mapper name", default=default_mapper_name(mapper_input_module, domain))
    elif mchoice == "identity":
        mapper_ep = "identity"
    else:
        mapper_ep = "identity"

    default_id = default_stream_id(domain, dataset or "dataset", None)
    stream_id = choose_name("Stream id", default=default_id)

    return StreamSelection(
        provider=provider,
        dataset=dataset,
        source_id=source_id,
        create_source=create_source,
        loader_ep=loader_ep,
        loader_args=loader_args,
        pchoice=pchoice,
        parser_create_dto=parser_create_dto,
        dto_class=dto_class,
        dto_module=dto_module,
        parser_name=parser_name,
        parser_ep=parser_ep,
        domain=domain,
        create_domain=create_domain,
        mchoice=mchoice,
        mapper_create_dto=mapper_create_dto,
        mapper_input_class=mapper_input_class,
        mapper_input_module=mapper_input_module,
        mapper_name=mapper_name,
        mapper_ep=mapper_ep,
        stream_id=stream_id,
    )


def _build_stream_plan_from_selection(
    *,
    selection: StreamSelection,
    project_yaml: Path,
    plugin_root: Path | None,
) -> StreamPlan:
    parser_plan = _build_parser_plan(
        choice=selection.pchoice,
        create_dto=selection.parser_create_dto,
        dto_class=selection.dto_class,
        dto_module=selection.dto_module,
        parser_name=selection.parser_name,
        parser_ep=selection.parser_ep,
    )

    mapper_plan = _build_mapper_plan(
        choice=selection.mchoice,
        create_dto=selection.mapper_create_dto,
        input_class=selection.mapper_input_class,
        input_module=selection.mapper_input_module,
        mapper_name=selection.mapper_name,
        mapper_ep=selection.mapper_ep,
        domain=selection.domain,
    )

    return StreamPlan(
        provider=selection.provider,
        dataset=selection.dataset,
        source_id=selection.source_id,
        project_yaml=project_yaml,
        stream_id=selection.stream_id,
        root=plugin_root,
        create_source=selection.create_source,
        loader_ep=selection.loader_ep,
        loader_args=selection.loader_args,
        parser=parser_plan,
        mapper=mapper_plan,
        domain=selection.domain,
        create_domain=selection.create_domain,
    )


def handle(*, plugin_root: Path | None = None, workspace: WorkspaceContext | None = None) -> None:
    root_dir, pkg_name, _ = pkg_root(plugin_root)
    project_yaml = resolve_default_project_yaml(
        workspace) or resolve_project_yaml_path(root_dir)
    selection = _collect_stream_selection(
        plugin_root=plugin_root,
        pkg_name=pkg_name,
        project_yaml=project_yaml,
    )
    plan = _build_stream_plan_from_selection(
        selection=selection,
        project_yaml=project_yaml,
        plugin_root=plugin_root,
    )
    execute_stream_plan(plan)
