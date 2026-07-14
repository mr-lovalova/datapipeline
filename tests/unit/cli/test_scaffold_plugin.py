from pathlib import Path

import pytest
import yaml

from datapipeline.profiles.loader import profile_specs
from datapipeline.services.project import load_project
from datapipeline.services.scaffold.plugin import scaffold_plugin
from datapipeline.services.scaffold.templates import render
from datapipeline.services.streams.loader import load_streams

_TEMPLATES_ROOT = Path(__file__).parents[3] / "src" / "datapipeline" / "templates"
_PLUGIN_SKELETON_ROOT = _TEMPLATES_ROOT / "plugin_skeleton"


def test_template_sources_do_not_contain_generated_files() -> None:
    generated = sorted(
        path.relative_to(_TEMPLATES_ROOT)
        for path in _TEMPLATES_ROOT.rglob("*")
        if path.name in {"__pycache__", ".DS_Store"} or path.suffix in {".pyc", ".pyo"}
    )

    assert generated == []


@pytest.mark.parametrize(
    "template",
    ["loaders/basic.py.j2", "loader_synthetic.py.j2"],
)
def test_loader_stubs_render_valid_python(template: str) -> None:
    source = render(template, CLASS_NAME="ExampleLoader")

    compile(source, template, "exec")


def test_demo_stream_templates_do_not_define_record_transforms() -> None:
    streams_root = _TEMPLATES_ROOT / "demo_skeleton" / "demo" / "streams"

    for path in streams_root.glob("*.yaml"):
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert "record" not in data, path


def test_demo_stream_catalog_matches_config_models() -> None:
    project = _TEMPLATES_ROOT / "demo_skeleton" / "demo" / "project.yaml"

    config = load_streams(load_project(project))

    assert len(config.sources) == 2
    assert len(config.ingests) == 5
    assert len(config.streams) == 5


def test_plugin_template_has_one_minimal_dataset() -> None:
    assert (_PLUGIN_SKELETON_ROOT / "your-dataset").is_dir()
    assert not (_PLUGIN_SKELETON_ROOT / "your-interim-data-builder").exists()
    assert not (_PLUGIN_SKELETON_ROOT / "_dataset_base").exists()
    assert not (_PLUGIN_SKELETON_ROOT / "reference").exists()


def test_template_profiles_separate_builds_from_runtime_actions() -> None:
    demo_expected = {
        "build.defaults.yaml",
        "build.metadata.yaml",
        "build.scaler.yaml",
        "build.schema.yaml",
        "build.vector_inputs.yaml",
        "inspect.coverage.yaml",
        "inspect.defaults.yaml",
        "inspect.matrix.yaml",
        "serve.defaults.yaml",
        "serve.dataset.yaml",
    }
    dataset_profiles = _PLUGIN_SKELETON_ROOT / "your-dataset" / "profiles"
    demo_profiles = _TEMPLATES_ROOT / "demo_skeleton" / "demo" / "profiles"

    assert {path.name for path in dataset_profiles.glob("*.yaml")} == {
        "build.schema.yaml",
        "inspect.coverage.yaml",
        "inspect.matrix.yaml",
        "serve.dataset.yaml",
        "serve.defaults.yaml",
    }
    assert {path.name for path in demo_profiles.glob("*.yaml")} == demo_expected

    project = load_project(_PLUGIN_SKELETON_ROOT / "your-dataset" / "project.yaml")
    assert [
        (profile.cmd, profile.name, profile.target)
        for profile in profile_specs(project)
    ] == [
        ("serve", "dataset", "pipeline"),
        ("build", "schema", "schema"),
        ("inspect", "coverage", "coverage"),
        ("inspect", "matrix", "matrix"),
    ]


def test_scaffold_plugin_normalizes_hyphenated_name(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    scaffold_plugin("test-datapipeline", tmp_path)

    plugin_root = tmp_path / "test-datapipeline"
    dataset_root = plugin_root / "your-dataset"
    assert (plugin_root / "src" / "test_datapipeline").is_dir()
    assert (dataset_root / ".env.example").is_file()
    assert not (plugin_root / "_dataset_base").exists()
    assert not (plugin_root / "reference").exists()
    assert not (plugin_root / "your-interim-data-builder").exists()
    assert not (dataset_root / "operations").exists()
    assert (dataset_root / "profiles" / "build.schema.yaml").is_file()
    assert (dataset_root / "profiles" / "inspect.coverage.yaml").is_file()
    assert (dataset_root / "profiles" / "inspect.matrix.yaml").is_file()
    assert (dataset_root / "profiles" / "serve.dataset.yaml").is_file()
    assert (dataset_root / "profiles" / "serve.defaults.yaml").is_file()

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
    assert set(cfg["datasets"]) == {"your-dataset"}


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
    assert set(cfg["datasets"]) == {"your-dataset"}


@pytest.mark.parametrize("name", ["data pipeline", "datapipeline"])
def test_scaffold_plugin_rejects_disallowed_names(name: str, tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        scaffold_plugin(name, tmp_path)

    assert exc.value.code == 1
