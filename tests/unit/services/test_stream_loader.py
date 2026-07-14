from pathlib import Path

import pytest

from datapipeline.services.project import load_project
from datapipeline.services.streams.ingest import build_source_from_spec
from datapipeline.services.streams.loader import load_streams


def _sources(project_yaml: Path):
    return load_streams(load_project(project_yaml)).sources


def _write_project_yaml(project_root: Path) -> Path:
    (project_root / "ingests").mkdir(parents=True, exist_ok=True)
    (project_root / "streams").mkdir(parents=True, exist_ok=True)
    project_yaml = project_root / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "artifact_revision: 1",
                "name: sample",
                "paths:",
                "  ingests: ./ingests",
                "  streams: streams",
                "  sources: sources",
                "  dataset: dataset.yaml",
                "  artifacts: build",
                "  operations: operations",
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
        "sample:\n  cadence: 1h\nfeatures: []\n",
        encoding="utf-8",
    )
    _write_source_yaml(project_root / "sources", "data/*.jsonl")
    project_yaml = _write_project_yaml(project_root)

    loaded = _sources(project_yaml)
    source = build_source_from_spec(
        loaded["sample.fs"],
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
    resolved_data = project_root / "demo" / "demo" / "data"
    resolved_data.mkdir(parents=True)
    (resolved_data / "rows.jsonl").write_text("{}", encoding="utf-8")
    (project_root / "streams").mkdir()
    (project_root / "tasks").mkdir()
    (project_root / "build").mkdir()
    (project_root / "dataset.yaml").write_text(
        "sample:\n  cadence: 1h\nfeatures: []\n", encoding="utf-8"
    )
    _write_source_yaml(project_root / "sources", "demo/demo/data/*.jsonl")
    project_yaml = _write_project_yaml(project_root)

    loaded = _sources(project_yaml)
    source = build_source_from_spec(
        loaded["sample.fs"],
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
    (project_root / "ingests").mkdir()
    (project_root / "tasks").mkdir()
    (project_root / "build").mkdir()
    (project_root / "data").mkdir()
    (project_root / "data" / "rows.jsonl").write_text("{}", encoding="utf-8")
    (project_root / "dataset.yaml").write_text(
        "sample:\n  cadence: 1h\nfeatures: []\n",
        encoding="utf-8",
    )
    _write_named_source_yaml(project_root / "sources", "local.yaml", "local.fs")
    _write_named_source_yaml(common_root / "sources", "common.yaml", "common.fs")
    project_yaml = project_root / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "artifact_revision: 1",
                "name: sample",
                "paths:",
                "  ingests: ./ingests",
                "  streams: streams",
                "  sources:",
                "    - sources",
                "    - ../common/sources",
                "  dataset: dataset.yaml",
                "  artifacts: build",
                "  operations: operations",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    loaded = _sources(project_yaml)

    assert sorted(loaded) == ["common.fs", "local.fs"]


def test_load_sources_rejects_duplicate_source_ids_across_roots(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    project_root = workspace / "project"
    common_root = workspace / "common"
    (project_root / "streams").mkdir(parents=True)
    (project_root / "ingests").mkdir()
    (project_root / "tasks").mkdir()
    (project_root / "build").mkdir()
    (project_root / "data").mkdir()
    (project_root / "data" / "rows.jsonl").write_text("{}", encoding="utf-8")
    (project_root / "dataset.yaml").write_text(
        "sample:\n  cadence: 1h\nfeatures: []\n",
        encoding="utf-8",
    )
    _write_named_source_yaml(project_root / "sources", "local.yaml", "same.fs")
    _write_named_source_yaml(
        common_root / "sources",
        "common.yaml",
        '" same.fs "',
    )
    project_yaml = project_root / "project.yaml"
    project_yaml.write_text(
        "\n".join(
            [
                "version: 1",
                "artifact_revision: 1",
                "name: sample",
                "paths:",
                "  ingests: ./ingests",
                "  streams: streams",
                "  sources:",
                "    - sources",
                "    - ../common/sources",
                "  dataset: dataset.yaml",
                "  artifacts: build",
                "  operations: operations",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate source id 'same.fs'"):
        _sources(project_yaml)


def test_load_sources_keys_by_interpolated_id(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_named_source_yaml(
        project_root / "sources",
        "source.yaml",
        "${configured_id}",
    )
    project_yaml = _write_project_yaml(project_root)
    project_yaml.write_text(
        project_yaml.read_text(encoding="utf-8")
        + 'globals:\n  configured_id: " canonical.source "\n',
        encoding="utf-8",
    )

    loaded = _sources(project_yaml)

    assert list(loaded) == ["canonical.source"]
    assert loaded["canonical.source"].id == "canonical.source"


def test_load_sources_rejects_non_mapping_yaml(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    sources_dir = project_root / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "bad.yaml").write_text(
        "- not\n- a\n- mapping\n",
        encoding="utf-8",
    )
    project_yaml = _write_project_yaml(project_root)

    with pytest.raises(TypeError, match="Top-level YAML .* must be a mapping"):
        _sources(project_yaml)


def test_load_sources_rejects_missing_id(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    sources_dir = project_root / "sources"
    sources_dir.mkdir(parents=True)
    (sources_dir / "bad.yaml").write_text(
        "parser:\n  entrypoint: identity\nloader:\n  entrypoint: core.io\n",
        encoding="utf-8",
    )
    project_yaml = _write_project_yaml(project_root)

    with pytest.raises(ValueError, match="Missing 'id' in source file"):
        _sources(project_yaml)
