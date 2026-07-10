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
from datapipeline.services.constants import DEFAULT_TEMPORAL_RECORD_PARSER_EP
from datapipeline.services.scaffold.layout import (
    dto_class_name,
    default_parser_name,
    default_mapper_name,
    dto_module_path,
    default_stream_id_for_source,
    LABEL_DTO_FOR_PARSER,
    LABEL_DTO_FOR_MAPPER,
    LABEL_DOMAIN_TO_MAP,
    LABEL_MAPPER_INPUT,
    default_mapper_name_for_identity,
    source_id_parts,
)
from datapipeline.services.scaffold.stream_plan import (
    StreamPlan,
    ParserPlan,
    MapperPlan,
    execute_stream_plan,
)
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
    parser: ParserPlan
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
    base = [
        ("create", "Create new parser (default)"),
        ("temporal_record", "Temporal record rehydration"),
        ("identity", "Identity parser"),
    ]
    if parsers:
        return [base[0], ("existing", "Select existing parser"), *base[1:]]
    return base


def _mapper_menu_options(mappers: dict[str, str]) -> list[tuple[str, str]]:
    base = [("create", "Create new mapper (default)"), ("identity", "Identity mapper")]
    if mappers:
        return [base[0], ("existing", "Select existing mapper"), base[1]]
    return base


def _build_mapper_plan(selection: StreamSelection) -> MapperPlan:
    if selection.mchoice == "create":
        return MapperPlan(
            create=True,
            create_dto=selection.mapper_create_dto,
            input_class=selection.mapper_input_class,
            input_module=selection.mapper_input_module,
            mapper_name=selection.mapper_name,
            domain=selection.domain,
        )
    if selection.mchoice == "existing":
        return MapperPlan(
            create=False,
            mapper_ep=selection.mapper_ep,
            domain=selection.domain,
        )
    return MapperPlan(create=False, mapper_ep="identity", domain=selection.domain)


def _select_new_source_loader() -> tuple[str, dict]:
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
        return default_loader_config(choice, fmt)
    return prompt_required("Loader entrypoint"), {}


def _select_parser_plan(
    plugin_root: Path | None,
    pkg_name: str,
    provider: str,
    dataset: str,
) -> ParserPlan:
    parsers = list_parsers(root=plugin_root)
    parser_choice = pick_from_menu("Parser:", _parser_menu_options(parsers))

    if parser_choice == "existing":
        parser_ep = pick_from_menu(
            "Select parser entrypoint:",
            [(k, k) for k in sorted(parsers.keys())],
        )
        return ParserPlan(create=False, parser_ep=parser_ep)

    if parser_choice == "create":
        dto_default = None
        if provider and dataset:
            dto_default = dto_class_name(f"{provider}_{dataset}")
        dto_class, create_dto = choose_existing_or_create_name(
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
        return ParserPlan(
            create=True,
            create_dto=create_dto,
            dto_class=dto_class,
            dto_module=dto_module_path(pkg_name, dto_class),
            parser_name=parser_name,
        )

    if parser_choice == "temporal_record":
        return ParserPlan(create=False, parser_ep=DEFAULT_TEMPORAL_RECORD_PARSER_EP)

    return ParserPlan(create=False, parser_ep="identity")


def _collect_stream_selection(
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
    mapper_create_dto = False

    mapper_input_class = None
    mapper_input_module = None
    loader_ep = None
    loader_args = None
    mapper_ep = None
    mapper_name = None
    parser_plan = ParserPlan(create=False, parser_ep="identity")
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
        source_provider, source_dataset, _source_variant = source_id_parts(source_id)
        provider = source_provider or provider
        dataset = source_dataset or dataset
    else:
        source_id_default = f"{provider}.{dataset}"
        source_id = choose_name("Source id", default=source_id_default)
        create_source = True

        loader_ep, loader_args = _select_new_source_loader()

        parser_plan = _select_parser_plan(plugin_root, pkg_name, provider, dataset)

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
            dto_class = parser_plan.dto_class
            dto_module = parser_plan.dto_module
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
                "Mapper name", default=default_mapper_name(mapper_input_module, domain)
            )
    elif mchoice == "identity":
        mapper_ep = "identity"
    else:
        mapper_ep = "identity"

    default_id = default_stream_id_for_source(domain, source_id)
    stream_id = choose_name("Stream id", default=default_id)

    return StreamSelection(
        provider=provider,
        dataset=dataset,
        source_id=source_id,
        create_source=create_source,
        loader_ep=loader_ep,
        loader_args=loader_args,
        parser=parser_plan,
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
    selection: StreamSelection,
    project_yaml: Path,
    plugin_root: Path | None,
) -> StreamPlan:
    mapper_plan = _build_mapper_plan(selection)

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
        parser=selection.parser,
        mapper=mapper_plan,
        domain=selection.domain,
        create_domain=selection.create_domain,
    )


def handle(
    *, plugin_root: Path | None = None, workspace: WorkspaceContext | None = None
) -> None:
    root_dir, pkg_name, _ = pkg_root(plugin_root)
    project_yaml = resolve_default_project_yaml(workspace) or resolve_project_yaml_path(
        root_dir
    )
    selection = _collect_stream_selection(plugin_root, pkg_name, project_yaml)
    plan = _build_stream_plan_from_selection(selection, project_yaml, plugin_root)
    execute_stream_plan(plan)
