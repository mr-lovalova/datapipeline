from dataclasses import dataclass
from pathlib import Path

from datapipeline.services.scaffold.domain import create_domain
from datapipeline.services.scaffold.dto import create_dto
from datapipeline.services.scaffold.parser import create_parser
from datapipeline.services.scaffold.mapper import create_mapper
from datapipeline.services.scaffold.source_yaml import create_source_yaml
from datapipeline.services.scaffold.contract_yaml import write_ingest_contract
from datapipeline.services.scaffold.discovery import list_dtos
from datapipeline.services.scaffold.utils import error_exit, status


@dataclass
class ParserPlan:
    create: bool
    create_dto: bool = False
    dto_class: str | None = None
    dto_module: str | None = None
    parser_name: str | None = None
    parser_ep: str | None = None


@dataclass
class MapperPlan:
    create: bool
    create_dto: bool = False
    input_class: str | None = None
    input_module: str | None = None
    mapper_name: str | None = None
    mapper_ep: str | None = None
    domain: str | None = None


@dataclass
class StreamPlan:
    provider: str
    dataset: str
    source_id: str
    project_yaml: Path
    stream_id: str
    root: Path | None
    create_source: bool
    loader_ep: str | None = None
    loader_args: dict | None = None
    parser: ParserPlan | None = None
    mapper: MapperPlan | None = None
    domain: str | None = None
    create_domain: bool = False


def execute_stream_plan(plan: StreamPlan) -> None:
    if plan.create_domain and plan.domain:
        create_domain(domain=plan.domain, root=plan.root)

    parser_ep = None
    if plan.parser:
        if plan.parser.create:
            if plan.parser.dto_class and plan.parser.create_dto:
                create_dto(name=plan.parser.dto_class, root=plan.root)
            dto_module = plan.parser.dto_module or list_dtos(root=plan.root).get(plan.parser.dto_class or "")
            if not dto_module:
                error_exit("Failed to resolve DTO module.")
            parser_ep = create_parser(
                name=plan.parser.parser_name or "parser",
                dto_class=plan.parser.dto_class or "DTO",
                dto_module=dto_module,
                root=plan.root,
            )
        else:
            parser_ep = plan.parser.parser_ep

    mapper_ep = None
    if plan.mapper:
        if plan.mapper.create:
            if plan.mapper.input_class and plan.mapper.create_dto:
                create_dto(name=plan.mapper.input_class, root=plan.root)
            input_module = plan.mapper.input_module
            if not input_module and plan.mapper.input_class:
                input_module = list_dtos(root=plan.root).get(plan.mapper.input_class)
            if not input_module:
                error_exit("Failed to resolve mapper input module.")
            mapper_ep = create_mapper(
                name=plan.mapper.mapper_name or "mapper",
                input_class=plan.mapper.input_class or "Record",
                input_module=input_module,
                domain=plan.mapper.domain or plan.domain or "domain",
                root=plan.root,
            )
        else:
            mapper_ep = plan.mapper.mapper_ep

    if plan.create_source and plan.loader_ep and plan.loader_args is not None:
        create_source_yaml(
            provider=plan.provider,
            dataset=plan.dataset,
            loader_ep=plan.loader_ep,
            loader_args=plan.loader_args,
            parser_ep=parser_ep or "identity",
            root=plan.root,
            project_yaml=plan.project_yaml,
        )

    write_ingest_contract(
        project_yaml=plan.project_yaml,
        stream_id=plan.stream_id,
        source=plan.source_id,
        mapper_entrypoint=mapper_ep or "identity",
    )
    status("ok", "Stream created.")
