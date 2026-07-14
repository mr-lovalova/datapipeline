from pathlib import Path
from types import MappingProxyType

from datapipeline.config.project import ProjectConfig
from datapipeline.services.config_refs import (
    interpolate_config_vars,
    merged_project_env,
    project_vars_from_data,
    resolve_config_refs,
)
from datapipeline.services.definitions import ProjectManifest
from datapipeline.services.path_policy import resolve_project_path
from datapipeline.utils.load import read_yaml_document


def _config_roots(project_yaml: Path, value: str | list[str]) -> tuple[Path, ...]:
    paths = value if isinstance(value, list) else [value]
    return tuple(_pipeline_config_path(project_yaml, path) for path in paths)


def _pipeline_config_path(project_yaml: Path, value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = project_yaml.parent / path
    return path.resolve()


def load_project(project_yaml: Path) -> ProjectManifest:
    path = project_yaml.resolve()
    document = read_yaml_document(path)
    environment = merged_project_env(path)
    data = resolve_config_refs(document.data, project_yaml=path, env=environment)
    variables = project_vars_from_data(data)
    raw_paths = data.get("paths")
    if isinstance(raw_paths, dict):
        data["paths"] = interpolate_config_vars(raw_paths, variables)
    config = ProjectConfig.model_validate(data)

    operations_dir = (
        _pipeline_config_path(path, config.paths.operations)
        if config.paths.operations is not None
        else None
    )
    return ProjectManifest(
        path=path,
        document=document,
        config=config,
        variables=MappingProxyType(variables),
        environment=MappingProxyType(environment),
        ingest_dirs=_config_roots(path, config.paths.ingests),
        stream_dirs=_config_roots(path, config.paths.streams),
        source_dirs=_config_roots(path, config.paths.sources),
        dataset_path=_pipeline_config_path(path, config.paths.dataset),
        artifacts_root=resolve_project_path(path, config.paths.artifacts),
        operations_dir=operations_dir,
        profiles_dir=resolve_project_path(path, config.paths.profiles or "./profiles"),
    )
