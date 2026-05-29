from pathlib import Path
from typing import Optional

from datapipeline.services.config_refs import (
    interpolate_config_vars,
    project_vars_from_data,
    resolve_config_refs,
)
from datapipeline.services.path_policy import resolve_project_path
from datapipeline.utils.load import load_yaml
from datapipeline.config.project import ProjectConfig


_DEFAULT_DOTENV_EXAMPLE = (
    "# Copy this file to .env next to project.yaml for local dataset-specific secrets.\n"
    "RAW_ROOT=\n"
)


def read_project(project_yaml: Path) -> ProjectConfig:
    data = resolve_config_refs(load_yaml(project_yaml), project_yaml=project_yaml)
    paths = data.get("paths")
    vars_ = project_vars_from_data(data)
    if isinstance(paths, dict) and vars_:
        data["paths"] = interpolate_config_vars(paths, vars_)
    return ProjectConfig.model_validate(data)


def streams_dir(project_yaml: Path) -> Path:
    return streams_dirs(project_yaml)[0]


def streams_dirs(project_yaml: Path) -> list[Path]:
    cfg = read_project(project_yaml)
    paths = (
        cfg.paths.streams
        if isinstance(cfg.paths.streams, list)
        else [cfg.paths.streams]
    )
    out: list[Path] = []
    for raw_path in paths:
        p = resolve_project_path(project_yaml, raw_path)
        if not p.exists() or not p.is_dir():
            raise FileNotFoundError(f"streams dir not found: {p}")
        out.append(p)
    return out


def ingests_dir(project_yaml: Path) -> Path:
    return ingests_dirs(project_yaml)[0]


def ingests_dirs(project_yaml: Path) -> list[Path]:
    cfg = read_project(project_yaml)
    paths = (
        cfg.paths.ingests
        if isinstance(cfg.paths.ingests, list)
        else [cfg.paths.ingests]
    )
    out: list[Path] = []
    for raw_path in paths:
        p = resolve_project_path(project_yaml, raw_path)
        if not p.exists() or not p.is_dir():
            raise FileNotFoundError(f"ingests dir not found: {p}")
        out.append(p)
    return out


def sources_dir(project_yaml: Path) -> Path:
    return sources_dirs(project_yaml)[0]


def sources_dirs(project_yaml: Path) -> list[Path]:
    cfg = read_project(project_yaml)
    paths = (
        cfg.paths.sources
        if isinstance(cfg.paths.sources, list)
        else [cfg.paths.sources]
    )
    out: list[Path] = []
    for raw_path in paths:
        p = resolve_project_path(project_yaml, raw_path)
        if not p.exists() or not p.is_dir():
            raise FileNotFoundError(f"sources dir not found: {p}")
        out.append(p)
    return out


def tasks_dir(project_yaml: Path) -> Path:
    """Return the resolved path to the tasks directory (project.paths.tasks)."""

    cfg = read_project(project_yaml)
    tasks_path = getattr(cfg.paths, "tasks", None)
    if not tasks_path:
        raise FileNotFoundError("project.paths.tasks must point to a tasks directory.")
    p = resolve_project_path(project_yaml, tasks_path)
    if not p.exists() or not p.is_dir():
        raise FileNotFoundError(f"tasks directory not found: {p}")
    return p


def profiles_dir(project_yaml: Path) -> Path:
    """Return the resolved profiles directory.

    Resolution order:
    1) project.paths.profiles when set
    2) ./profiles relative to project.yaml
    """
    cfg = read_project(project_yaml)
    profiles_path = getattr(cfg.paths, "profiles", None) or "./profiles"
    return resolve_project_path(project_yaml, profiles_path)


def ensure_project_scaffold(project_yaml: Path) -> None:
    """Ensure a minimal project scaffold exists.

    - Creates parent directories and a default project.yaml if missing.
    - Ensures the `paths.streams` and `paths.sources` directories exist.
    - Never overwrites existing files.
    """
    # Create default project.yaml if missing
    if not project_yaml.exists():
        project_yaml.parent.mkdir(parents=True, exist_ok=True)
        default = (
            "version: 1\n"
            "name: default\n"
            "paths:\n"
            "  ingests: ./ingests\n"
            "  streams: ./streams\n"
            "  sources: ./sources\n"
            "  dataset: dataset.yaml\n"
            "  postprocess: postprocess.yaml\n"
            "  artifacts: ../artifacts/default\n"
            "  tasks: ./tasks\n"
            "  profiles: ./profiles\n"
            "globals:\n"
            "  start_time: 2021-01-01T00:00:00Z\n"
            "  end_time: 2021-12-31T23:00:00Z\n"
        )
        project_yaml.write_text(default, encoding="utf-8")

    # Ensure paths exist based on the (possibly newly created) project file
    try:
        cfg = read_project(project_yaml)
        ingest_paths = (
            cfg.paths.ingests
            if isinstance(cfg.paths.ingests, list)
            else [cfg.paths.ingests]
        )
        for raw_ingest_path in ingest_paths:
            ingests = resolve_project_path(project_yaml, raw_ingest_path)
            ingests.mkdir(parents=True, exist_ok=True)

        stream_paths = (
            cfg.paths.streams
            if isinstance(cfg.paths.streams, list)
            else [cfg.paths.streams]
        )
        for raw_stream_path in stream_paths:
            streams = resolve_project_path(project_yaml, raw_stream_path)
            streams.mkdir(parents=True, exist_ok=True)

        source_paths = (
            cfg.paths.sources
            if isinstance(cfg.paths.sources, list)
            else [cfg.paths.sources]
        )
        for raw_source_path in source_paths:
            sources = resolve_project_path(project_yaml, raw_source_path)
            sources.mkdir(parents=True, exist_ok=True)

        tasks = getattr(cfg.paths, "tasks", None)
        if tasks:
            tasks_path = resolve_project_path(project_yaml, tasks)
            tasks_path.mkdir(parents=True, exist_ok=True)
        profiles = getattr(cfg.paths, "profiles", None)
        profiles_path = (
            resolve_project_path(project_yaml, profiles)
            if profiles
            else resolve_project_path(project_yaml, "./profiles")
        )
        profiles_path.mkdir(parents=True, exist_ok=True)
        dotenv_example = project_yaml.parent / ".env.example"
        if not dotenv_example.exists():
            dotenv_example.write_text(_DEFAULT_DOTENV_EXAMPLE, encoding="utf-8")
    except Exception:
        # If the file is malformed, leave it to callers to report; this helper
        # is best-effort to create a sensible starting point.
        pass


def resolve_project_yaml_path(plugin_root: Path) -> Path:
    """Return a best-effort project.yaml path for scaffolding.

    Resolution order:
    1) <plugin_root>/example/project.yaml
    2) <plugin_root>/config/project.yaml
    3) <plugin_root>/config/datasets/default/project.yaml
    4) Fallback: <plugin_root>/example/project.yaml
    """
    candidates = [
        plugin_root / "example" / "project.yaml",
        plugin_root / "config" / "project.yaml",
        plugin_root / "config" / "datasets" / "default" / "project.yaml",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    # Default to the first candidate; callers may scaffold a new project there.
    return candidates[0]
