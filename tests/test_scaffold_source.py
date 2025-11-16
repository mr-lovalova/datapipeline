from __future__ import annotations

from pathlib import Path

from datapipeline.services.scaffold.source import create_source


def _create_plugin(tmp_path: Path) -> Path:
    root = tmp_path / "sample_plugin"
    pkg_dir = root / "src" / "sample_plugin"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
    (root / "pyproject.toml").write_text(
        '[project]\nname = "sample-plugin"\nversion = "0.0.0"\n',
        encoding="utf-8",
    )
    return root


def _write_project_yaml(path: Path, sources_dir: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = (
        "version: 1\n"
        "paths:\n"
        "  streams: ../../contracts\n"
        f"  sources: {sources_dir}\n"
        "  dataset: dataset.yaml\n"
        "  postprocess: postprocess.yaml\n"
        "  artifacts: ../../build\n"
        "  build: builds\n"
        "  run: runs\n"
        "globals: {}\n"
    )
    path.write_text(content, encoding="utf-8")


def test_create_source_uses_external_config_root(tmp_path: Path) -> None:
    plugin_root = _create_plugin(tmp_path)
    workspace_root = tmp_path / "workspace"
    config_root = workspace_root / "config" / "datasets" / "production"
    sources_dir = workspace_root / "config" / "sources"
    sources_dir.mkdir(parents=True)
    _write_project_yaml(config_root / "project.yaml", sources_dir)

    create_source(
        provider="demo",
        dataset="weather",
        transport="fs",
        format="csv",
        root=plugin_root,
        config_root=config_root,
    )

    expected = sources_dir / "demo.weather.yaml"
    assert expected.exists(), f"expected scaffolded source at {expected}"
    plugin_sources = plugin_root / "config" / "sources" / "demo.weather.yaml"
    assert not plugin_sources.exists(), "source YAML should not be written inside the plugin config"


def test_create_source_accepts_project_file_override(tmp_path: Path) -> None:
    plugin_root = _create_plugin(tmp_path)
    workspace_root = tmp_path / "workspace"
    config_root = workspace_root / "config"
    sources_dir = workspace_root / "config_sources"
    sources_dir.mkdir(parents=True)
    project_path = config_root / "datasets.yaml"
    _write_project_yaml(project_path, sources_dir)

    create_source(
        provider="demo",
        dataset="traffic",
        transport="fs",
        format="json",
        root=plugin_root,
        config_root=project_path,
    )

    expected = sources_dir / "demo.traffic.yaml"
    assert expected.exists(), f"expected scaffolded source at {expected}"
