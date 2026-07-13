from pathlib import Path

import pytest

from datapipeline.config.catalog import SourceConfig
from datapipeline.services.bootstrap.core import _load_sources_from_dir
from datapipeline.services.streams.ingest import build_source_from_spec


def _write_project_yaml(project_root: Path) -> Path:
    project_yaml = project_root / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: sample",
                "paths:",
                "  ingests: ./ingests",
                "  streams: streams",
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
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_named_source_yaml(sources_dir: Path, filename: str, source_id: str) -> None:
    sources_dir.mkdir(parents=True, exist_ok=True)
    (sources_dir / filename).write_text(
        "\n".join(
            [
                f"id: {source_id}",
                "parser:",
                "  entrypoint: identity",
                "  args: {}",
                "loader:",
                "  entrypoint: core.io",
                "  args:",
                "    transport: fs",
                "    format: jsonl",
                "    path: data/rows.jsonl",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_load_sources_resolves_fs_path_from_project_root(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    (project_root / "data").mkdir(parents=True)
    (project_root / "data" / "rows.jsonl").write_text("{}", encoding="utf-8")
    (project_root / "streams").mkdir()
    (project_root / "tasks").mkdir()
    (project_root / "build").mkdir()
    (project_root / "dataset.yaml").write_text(
        "group_by: 1h\nfeatures: []\n",
        encoding="utf-8",
    )
    (project_root / "postprocess.yaml").write_text("[]\n", encoding="utf-8")
    _write_source_yaml(project_root / "sources", "data/*.jsonl")
    project_yaml = _write_project_yaml(project_root)

    loaded = _load_sources_from_dir(project_yaml, vars_={})
    source = build_source_from_spec(
        SourceConfig.model_validate(loaded["sample.fs"]),
        project_yaml=project_yaml,
    )
    path_value = source.loader.transport.pattern
    assert path_value == str((project_root / "data" / "*.jsonl").resolve())


def test_load_sources_resolves_fs_path_relative_to_project_root_only(
    tmp_path: Path,
) -> None:
    project_root = tmp_path / "workspace" / "demo" / "demo"
    (project_root / "data").mkdir(parents=True)
    (project_root / "data" / "rows.jsonl").write_text("{}", encoding="utf-8")
    (project_root / "streams").mkdir()
    (project_root / "tasks").mkdir()
    (project_root / "build").mkdir()
    (project_root / "dataset.yaml").write_text(
        "group_by: 1h\nfeatures: []\n", encoding="utf-8"
    )
    (project_root / "postprocess.yaml").write_text("[]\n", encoding="utf-8")
    _write_source_yaml(project_root / "sources", "demo/demo/data/*.jsonl")
    project_yaml = _write_project_yaml(project_root)

    loaded = _load_sources_from_dir(project_yaml, vars_={})
    source = build_source_from_spec(
        SourceConfig.model_validate(loaded["sample.fs"]),
        project_yaml=project_yaml,
    )
    path_value = source.loader.transport.pattern
    assert path_value == str(
        (project_root / "demo" / "demo" / "data" / "*.jsonl").resolve()
    )


def test_load_sources_reads_multiple_source_roots(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    project_root = workspace / "project"
    common_root = workspace / "common"
    (project_root / "streams").mkdir(parents=True)
    (project_root / "tasks").mkdir()
    (project_root / "build").mkdir()
    (project_root / "data").mkdir()
    (project_root / "data" / "rows.jsonl").write_text("{}", encoding="utf-8")
    (project_root / "dataset.yaml").write_text(
        "group_by: 1h\nfeatures: []\n",
        encoding="utf-8",
    )
    (project_root / "postprocess.yaml").write_text("[]\n", encoding="utf-8")
    _write_named_source_yaml(project_root / "sources", "local.yaml", "local.fs")
    _write_named_source_yaml(common_root / "sources", "common.yaml", "common.fs")
    project_yaml = project_root / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: sample",
                "paths:",
                "  ingests: ./ingests",
                "  streams: streams",
                "  sources:",
                "    - sources",
                "    - ../common/sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: build",
                "  tasks: tasks",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    loaded = _load_sources_from_dir(project_yaml, vars_={})

    assert sorted(loaded) == ["common.fs", "local.fs"]


def test_load_sources_rejects_duplicate_source_ids_across_roots(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    project_root = workspace / "project"
    common_root = workspace / "common"
    (project_root / "streams").mkdir(parents=True)
    (project_root / "tasks").mkdir()
    (project_root / "build").mkdir()
    (project_root / "data").mkdir()
    (project_root / "data" / "rows.jsonl").write_text("{}", encoding="utf-8")
    (project_root / "dataset.yaml").write_text(
        "group_by: 1h\nfeatures: []\n",
        encoding="utf-8",
    )
    (project_root / "postprocess.yaml").write_text("[]\n", encoding="utf-8")
    _write_named_source_yaml(project_root / "sources", "local.yaml", "same.fs")
    _write_named_source_yaml(common_root / "sources", "common.yaml", "same.fs")
    project_yaml = project_root / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "name: sample",
                "paths:",
                "  ingests: ./ingests",
                "  streams: streams",
                "  sources:",
                "    - sources",
                "    - ../common/sources",
                "  dataset: dataset.yaml",
                "  postprocess: postprocess.yaml",
                "  artifacts: build",
                "  tasks: tasks",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate source id 'same.fs'"):
        _load_sources_from_dir(project_yaml, vars_={})
