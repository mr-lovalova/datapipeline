from pathlib import Path

from datapipeline.services.project import load_project


_DEFAULT_DOTENV_EXAMPLE = (
    "# Copy this file to .env next to project.yaml for local dataset-specific secrets.\n"
    "RAW_ROOT=\n"
)


def ensure_project_scaffold(project_yaml: Path) -> None:
    """Ensure a minimal project scaffold exists.

    - Creates parent directories and a default project.yaml if missing.
    - Ensures configured stream, source, operation, and profile directories exist.
    - Never overwrites existing files.
    """
    # Create default project.yaml if missing
    if not project_yaml.exists():
        project_yaml.parent.mkdir(parents=True, exist_ok=True)
        default = (
            "schema_version: 2\n"
            "artifact_revision: 1\n"
            "name: default\n"
            "paths:\n"
            "  streams: ./streams\n"
            "  sources: ./sources\n"
            "  dataset: dataset.yaml\n"
            "  artifacts: ../artifacts/default\n"
            "  profiles: ./profiles\n"
            "globals:\n"
            "  start_time: 2021-01-01T00:00:00Z\n"
            "  end_time: 2021-12-31T23:00:00Z\n"
        )
        project_yaml.write_text(default, encoding="utf-8")

    project = load_project(project_yaml)
    for streams in project.stream_dirs:
        streams.mkdir(parents=True, exist_ok=True)

    for sources in project.source_dirs:
        sources.mkdir(parents=True, exist_ok=True)

    if project.operations_dir is not None:
        project.operations_dir.mkdir(parents=True, exist_ok=True)
    project.profiles_dir.mkdir(parents=True, exist_ok=True)
    dotenv_example = project_yaml.parent / ".env.example"
    if not dotenv_example.exists():
        dotenv_example.write_text(_DEFAULT_DOTENV_EXAMPLE, encoding="utf-8")


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
