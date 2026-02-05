from pathlib import Path

import pytest

import yaml

from datapipeline.services.scaffold.plugin import scaffold_plugin


def test_scaffold_plugin_normalizes_hyphenated_name(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    scaffold_plugin("test-datapipeline", tmp_path)

    plugin_root = tmp_path / "test-datapipeline"
    assert (plugin_root / "src" / "test_datapipeline").is_dir()

    pyproject = (plugin_root / "pyproject.toml").read_text()
    assert 'name = "test-datapipeline"' in pyproject

    readme = (plugin_root / "README.md").read_text()
    assert "jerry plugin init test-datapipeline" in readme


def test_scaffold_plugin_moves_jerry_to_workspace(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    scaffold_plugin("test-datapipeline", tmp_path)

    workspace_jerry = tmp_path / "jerry.yaml"
    plugin_jerry = tmp_path / "test-datapipeline" / "jerry.yaml"

    assert workspace_jerry.is_file()
    assert not plugin_jerry.exists()

    cfg = yaml.safe_load(workspace_jerry.read_text())
    assert cfg["plugin_root"] == "test-datapipeline"
    assert cfg["datasets"]["your-dataset"] == "test-datapipeline/your-dataset/project.yaml"
    assert (
        cfg["datasets"]["interim-builder"]
        == "test-datapipeline/your-interim-data-builder/project.yaml"
    )


def test_scaffold_plugin_respects_outdir(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    outdir = tmp_path / "plugins"
    outdir.mkdir()

    scaffold_plugin("myplugin", outdir)

    workspace_jerry = tmp_path / "jerry.yaml"
    plugin_root = outdir / "myplugin"

    assert workspace_jerry.is_file()
    assert not (plugin_root / "jerry.yaml").exists()
    cfg = yaml.safe_load(workspace_jerry.read_text())
    assert cfg["plugin_root"] == "plugins/myplugin"
    assert cfg["datasets"]["your-dataset"] == "plugins/myplugin/your-dataset/project.yaml"
    assert (
        cfg["datasets"]["interim-builder"]
        == "plugins/myplugin/your-interim-data-builder/project.yaml"
    )


@pytest.mark.parametrize("name", ["data pipeline", "datapipeline"])
def test_scaffold_plugin_rejects_disallowed_names(name: str, tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        scaffold_plugin(name, tmp_path)

    assert exc.value.code == 1
