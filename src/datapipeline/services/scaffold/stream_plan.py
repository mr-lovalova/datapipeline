from dataclasses import dataclass
from pathlib import Path

from datapipeline.services.paths import pkg_root
from datapipeline.services.scaffold.domain import create_domain
from datapipeline.services.scaffold.dto import create_dto
from datapipeline.services.scaffold.mapper import create_mapper
from datapipeline.services.scaffold.parser import create_parser
from datapipeline.services.scaffold.source_yaml import (
    create_source_yaml,
    validate_source_id,
)
from datapipeline.services.scaffold.stream_yaml import write_source_stream
from datapipeline.services.scaffold.utils import status


@dataclass(frozen=True)
class PythonType:
    class_name: str
    module: str


@dataclass(frozen=True)
class ParserReference:
    entrypoint: str


@dataclass(frozen=True)
class ParserCreation:
    name: str
    dto: PythonType


ParserPlan = ParserReference | ParserCreation


@dataclass(frozen=True)
class MapperReference:
    entrypoint: str


@dataclass(frozen=True)
class MapperCreation:
    name: str
    input_type: PythonType


MapperPlan = MapperReference | MapperCreation


@dataclass(frozen=True)
class SourceReference:
    source_id: str


@dataclass(frozen=True)
class SourceCreation:
    source_id: str
    loader_entrypoint: str
    loader_args: dict[str, object]
    parser: ParserPlan


SourcePlan = SourceReference | SourceCreation


@dataclass(frozen=True)
class DomainReference:
    name: str


@dataclass(frozen=True)
class DomainCreation:
    name: str


DomainPlan = DomainReference | DomainCreation


@dataclass(frozen=True)
class StreamPlan:
    project_yaml: Path
    stream_id: str
    root: Path | None
    source: SourcePlan
    mapper: MapperPlan
    domain: DomainPlan
    dto_to_create: str | None


def execute_stream_plan(plan: StreamPlan) -> None:
    if isinstance(plan.source, SourceCreation):
        validate_source_id(plan.source.source_id)
    _, _, pyproject_path = pkg_root(plan.root)
    before_pyproject = pyproject_path.read_text(encoding="utf-8")

    if isinstance(plan.domain, DomainCreation):
        create_domain(domain=plan.domain.name, root=plan.root)

    if plan.dto_to_create is not None:
        create_dto(name=plan.dto_to_create, root=plan.root)

    if isinstance(plan.source, SourceCreation):
        if isinstance(plan.source.parser, ParserCreation):
            parser_entrypoint = create_parser(
                name=plan.source.parser.name,
                dto_class=plan.source.parser.dto.class_name,
                dto_module=plan.source.parser.dto.module,
                root=plan.root,
            )
        else:
            parser_entrypoint = plan.source.parser.entrypoint

    if isinstance(plan.mapper, MapperCreation):
        mapper_entrypoint = create_mapper(
            name=plan.mapper.name,
            input_class=plan.mapper.input_type.class_name,
            input_module=plan.mapper.input_type.module,
            domain=plan.domain.name,
            root=plan.root,
        )
    else:
        mapper_entrypoint = plan.mapper.entrypoint

    if isinstance(plan.source, SourceCreation):
        create_source_yaml(
            source_id=plan.source.source_id,
            loader_ep=plan.source.loader_entrypoint,
            loader_args=plan.source.loader_args,
            parser_ep=parser_entrypoint,
            root=plan.root,
            project_yaml=plan.project_yaml,
        )

    write_source_stream(
        project_yaml=plan.project_yaml,
        stream_id=plan.stream_id,
        source=plan.source.source_id,
        mapper_entrypoint=mapper_entrypoint,
    )
    status("ok", "Stream created.")

    after_pyproject = pyproject_path.read_text(encoding="utf-8")
    if after_pyproject != before_pyproject:
        status(
            "note",
            f"Entry points updated; reinstall plugin: pip install -e {pyproject_path.parent}",
        )
