from pathlib import Path

from datapipeline.services.bootstrap.core import _load_sources_from_dir


def _write_project_yaml(project_root: Path) -> Path:
    project_yaml = project_root / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: sample",
                "paths:",
                "  streams: contracts",
                "  sources: sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: build",
                "  tasks: tasks",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return project_yaml


def _write_source_yaml(sources_dir: Path, path_value: str) -> None:
    sources_dir.mkdir(parents=True, exist_ok=True)
    (sources_dir / "sample.yaml").write_text(
        "\n".join(
            [
                "id: sample.fs",
                "parser:",
                "  entrypoint: identity",
                "  args: {}",
                "loader:",
                "  entrypoint: core.io",
                "  args:",
                "    transport: fs",
                "    format: jsonl",
                f"    path: {path_value}",
                "    glob: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_load_sources_resolves_fs_path_from_project_root(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    (project_root / "data").mkdir(parents=True)
    (project_root / "data" / "rows.jsonl").write_text("{}", encoding="utf-8")
    (project_root / "contracts").mkdir()
    (project_root / "tasks").mkdir()
    (project_root / "build").mkdir()
    (project_root / "dataset.yaml").write_text("group_by: 1h\nfeatures: []\n", encoding="utf-8")
    (project_root / "postprocess.yaml").write_text("[]\n", encoding="utf-8")
    _write_source_yaml(project_root / "sources", "data/*.jsonl")
    project_yaml = _write_project_yaml(project_root)

    loaded = _load_sources_from_dir(project_yaml, vars_={})
    path_value = loaded["sample.fs"]["loader"]["args"]["path"]
    assert path_value == str((project_root / "data" / "*.jsonl").resolve())


def test_load_sources_supports_workspace_relative_legacy_paths(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    (workspace_root / "jerry.yaml").write_text("datasets: {}\n", encoding="utf-8")
    # Nested plugin root may also have its own jerry.yaml; resolver should still
    # try outer workspace roots so legacy workspace-relative demo paths work.
    (workspace_root / "demo" / "jerry.yaml").parent.mkdir(parents=True, exist_ok=True)
    (workspace_root / "demo" / "jerry.yaml").write_text("datasets: {}\n", encoding="utf-8")

    project_root = workspace_root / "demo" / "demo"
    (project_root / "data").mkdir(parents=True)
    (project_root / "data" / "rows.jsonl").write_text("{}", encoding="utf-8")
    (project_root / "contracts").mkdir()
    (project_root / "tasks").mkdir()
    (project_root / "build").mkdir()
    (project_root / "dataset.yaml").write_text("group_by: 1h\nfeatures: []\n", encoding="utf-8")
    (project_root / "postprocess.yaml").write_text("[]\n", encoding="utf-8")
    _write_source_yaml(project_root / "sources", "demo/demo/data/*.jsonl")
    project_yaml = _write_project_yaml(project_root)

    loaded = _load_sources_from_dir(project_yaml, vars_={})
    path_value = loaded["sample.fs"]["loader"]["args"]["path"]
    assert path_value == str((workspace_root / "demo" / "demo" / "data" / "*.jsonl").resolve())
