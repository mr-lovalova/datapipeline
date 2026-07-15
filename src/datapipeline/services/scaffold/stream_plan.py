from dataclasses import dataclass
from pathlib import Path

from datapipeline.config.streams import SourceStreamConfig
from datapipeline.services.paths import pkg_root
from datapipeline.services.project import load_project
from datapipeline.services.scaffold.domain import (
    create_domain,
    validate_domain_creation,
)
from datapipeline.services.scaffold.dto import create_dto, validate_dto_creation
from datapipeline.services.scaffold.layout import ep_key_from_name
from datapipeline.services.scaffold.mapper import (
    create_mapper,
    validate_mapper_creation,
)
from datapipeline.services.scaffold.parser import (
    create_parser,
    validate_parser_creation,
)
from datapipeline.services.scaffold.source_yaml import (
    create_source_yaml,
    validate_source_id,
)
from datapipeline.services.scaffold.stream_yaml import write_source_stream
from datapipeline.services.scaffold.utils import is_python_identifier


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


@dataclass(frozen=True)
class StreamPlanResult:
    path: Path
    entry_points_changed: bool


def _preflight_stream_plan(plan: StreamPlan) -> Path:
    if isinstance(plan.source, SourceCreation):
        validate_source_id(plan.source.source_id)
        if isinstance(plan.source.parser, ParserCreation):
            if not is_python_identifier(plan.source.parser.dto.class_name):
                raise ValueError("DTO name must be a valid Python identifier")
            validate_parser_creation(plan.source.parser.name, plan.root)

    if isinstance(plan.domain, DomainCreation):
        validate_domain_creation(plan.domain.name, plan.root)
    elif not is_python_identifier(plan.domain.name):
        raise ValueError("Domain name must be a valid Python identifier")
    if plan.dto_to_create is not None:
        validate_dto_creation(plan.dto_to_create, plan.root)
    if isinstance(plan.mapper, MapperCreation):
        validate_mapper_creation(plan.mapper.name, plan.root)

    _, _, pyproject_path = pkg_root(plan.root)

    mapper_entrypoint = (
        ep_key_from_name(plan.mapper.name)
        if isinstance(plan.mapper, MapperCreation)
        else plan.mapper.entrypoint
    )
    config = SourceStreamConfig.model_validate(
        {
            "id": plan.stream_id,
            "from": {"source": plan.source.source_id},
            "map": {"entrypoint": mapper_entrypoint},
        }
    )
    if plan.project_yaml.exists():
        project = load_project(plan.project_yaml)
        if isinstance(plan.source, SourceCreation):
            source_path = project.source_dirs[0] / f"{plan.source.source_id}.yaml"
            if source_path.exists():
                raise FileExistsError(f"{source_path} already exists")
        destination = project.stream_dirs[0] / f"{config.id}.yaml"
        if destination.exists():
            raise FileExistsError(f"{destination} already exists")
    return pyproject_path


def execute_stream_plan(plan: StreamPlan) -> StreamPlanResult:
    pyproject_path = _preflight_stream_plan(plan)
    before_pyproject = pyproject_path.read_text(encoding="utf-8")

    if isinstance(plan.domain, DomainCreation):
        create_domain(plan.domain.name, plan.root)

    if plan.dto_to_create is not None:
        create_dto(plan.dto_to_create, plan.root)

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

    path = write_source_stream(
        project_yaml=plan.project_yaml,
        stream_id=plan.stream_id,
        source=plan.source.source_id,
        mapper_entrypoint=mapper_entrypoint,
    )

    after_pyproject = pyproject_path.read_text(encoding="utf-8")
    return StreamPlanResult(
        path=path,
        entry_points_changed=after_pyproject != before_pyproject,
    )
