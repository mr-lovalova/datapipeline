from pathlib import Path

import pytest

import yaml

from datapipeline.services.scaffold.plugin import scaffold_plugin

_TEMPLATES_ROOT = Path(__file__).parents[3] / "src" / "datapipeline" / "templates"
_PLUGIN_SKELETON_ROOT = _TEMPLATES_ROOT / "plugin_skeleton"


def test_template_sources_do_not_contain_generated_files() -> None:
    generated = sorted(
        path.relative_to(_TEMPLATES_ROOT)
        for path in _TEMPLATES_ROOT.rglob("*")
        if path.name in {"__pycache__", ".DS_Store"} or path.suffix in {".pyc", ".pyo"}
    )

    assert generated == []


def test_demo_stream_templates_do_not_define_record_transforms() -> None:
    streams_root = _TEMPLATES_ROOT / "demo_skeleton" / "demo" / "streams"

    for path in streams_root.glob("*.yaml"):
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert "record" not in data, path


def test_dataset_template_base_does_not_overlap_concrete_overlays() -> None:
    dataset_base = _PLUGIN_SKELETON_ROOT / "_dataset_base"
    shared_paths = {
        path.relative_to(dataset_base)
        for path in dataset_base.rglob("*")
        if path.is_file()
    }

    assert shared_paths
    for overlay_name in ("your-dataset", "your-interim-data-builder"):
        overlay = _PLUGIN_SKELETON_ROOT / overlay_name
        overlay_paths = {
            path.relative_to(overlay) for path in overlay.rglob("*") if path.is_file()
        }
        assert shared_paths.isdisjoint(overlay_paths)


def test_scaffold_plugin_normalizes_hyphenated_name(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    scaffold_plugin("test-datapipeline", tmp_path)

    plugin_root = tmp_path / "test-datapipeline"
    dataset_root = plugin_root / "your-dataset"
    interim_root = plugin_root / "your-interim-data-builder"
    assert (plugin_root / "src" / "test_datapipeline").is_dir()
    assert (dataset_root / ".env.example").is_file()
    assert (interim_root / ".env.example").is_file()
    assert not (plugin_root / "_dataset_base").exists()

    source_base = _PLUGIN_SKELETON_ROOT / "_dataset_base"
    shared_paths = [
        path.relative_to(source_base)
        for path in source_base.rglob("*")
        if path.is_file()
    ]
    assert shared_paths
    for relative_path in shared_paths:
        dataset_file = dataset_root / relative_path
        interim_file = interim_root / relative_path
        assert dataset_file.is_file(), relative_path
        assert interim_file.is_file(), relative_path
        assert dataset_file.read_bytes() == interim_file.read_bytes()

    assert (dataset_root / "profiles" / "serve.splits.yaml").is_file()
    assert not (interim_root / "profiles" / "serve.splits.yaml").exists()
    assert (interim_root / "profiles" / "serve.all.yaml").is_file()
    assert not (dataset_root / "profiles" / "serve.all.yaml").exists()

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
    assert (
        cfg["datasets"]["your-dataset"] == "test-datapipeline/your-dataset/project.yaml"
    )
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
    assert (
        cfg["datasets"]["your-dataset"] == "plugins/myplugin/your-dataset/project.yaml"
    )
    assert (
        cfg["datasets"]["interim-builder"]
        == "plugins/myplugin/your-interim-data-builder/project.yaml"
    )


@pytest.mark.parametrize("name", ["data pipeline", "datapipeline"])
def test_scaffold_plugin_rejects_disallowed_names(name: str, tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        scaffold_plugin(name, tmp_path)

    assert exc.value.code == 1
