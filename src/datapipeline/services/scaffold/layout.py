from pathlib import Path
import re

from datapipeline.services.scaffold.templates import camel


def to_snake(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")


def ep_key_from_name(name: str) -> str:
    return to_snake(name)


# Directory names
DIR_DTOS = "dtos"
DIR_PARSERS = "parsers"
DIR_LOADERS = "loaders"
DIR_MAPPERS = "mappers"
DIR_DOMAINS = "domains"

# Template paths
TPL_DTO = "dto.py.j2"
TPL_PARSER = "parser.py.j2"
TPL_LOADER_BASIC = "loaders/basic.py.j2"
TPL_LOADER_SYNTHETIC = "loader_synthetic.py.j2"
TPL_MAPPER_INGEST = "mappers/ingest.py.j2"
TPL_MAPPER_COMPOSED = "mappers/composed.py.j2"
TPL_DOMAIN_RECORD = "record.py.j2"


def class_name_with_suffix(name: str, suffix: str) -> str:
    return f"{camel(name)}{suffix}"


def loader_class_name(name: str) -> str:
    return class_name_with_suffix(name, "Loader")


def domain_record_class(domain: str) -> str:
    return class_name_with_suffix(domain, "Record")


def loader_template_name(template: str) -> str:
    if template == "synthetic":
        return TPL_LOADER_SYNTHETIC
    return TPL_LOADER_BASIC


def dto_class_name(base: str) -> str:
    return class_name_with_suffix(base, "DTO")


def dto_module_path(package: str, dto_class: str) -> str:
    return f"{package}.{DIR_DTOS}.{to_snake(dto_class)}"


def default_parser_name(dto_class: str) -> str:
    return f"{dto_class}Parser"


def default_mapper_name(input_module: str, domain: str) -> str:
    input_mod = input_module.rsplit(".", 1)[-1]
    return f"map_{input_mod}_to_{domain}"


def default_stream_id(domain: str, dataset: str, variant: str | None = None) -> str:
    base = f"{slugify(domain)}.{slugify(dataset)}"
    return f"{base}.{slugify(variant)}" if variant else base


# Prompt labels (keep CLI wording consistent)
LABEL_DTO_FOR_PARSER = "DTO for parser"
LABEL_DTO_FOR_MAPPER = "DTO for mapper"
LABEL_DOMAIN_TO_MAP = "Domain"
LABEL_MAPPER_INPUT = "Mapper input"


def default_mapper_name_for_identity(domain: str) -> str:
    return f"map_identity_to_{slugify(domain)}"

def pyproject_path(root_dir: Path) -> Path:
    return root_dir / "pyproject.toml"


def module_path(package: str, group: str, module: str) -> str:
    return f"{package}.{group}.{module}"


def entrypoint_target(package: str, group: str, module: str, attr: str) -> str:
    return f"{module_path(package, group, module)}:{attr}"
