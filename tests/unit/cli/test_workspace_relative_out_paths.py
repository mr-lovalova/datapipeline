from pathlib import Path

from datapipeline.cli.commands import demo, plugin
from datapipeline.config.workspace import WorkspaceConfig, WorkspaceContext


def _workspace_at(root: Path) -> WorkspaceContext:
    cfg = WorkspaceConfig.model_validate({})
    return WorkspaceContext(file_path=root / "jerry.yaml", config=cfg)


def test_plugin_init_out_path_is_workspace_relative(tmp_path: Path, monkeypatch) -> None:
    workspace = _workspace_at(tmp_path)
    (tmp_path / "subdir").mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path / "subdir")

    calls: dict[str, Path] = {}

    def fake_scaffold_plugin(name: str, outdir: Path) -> None:
        calls["name"] = name
        calls["outdir"] = outdir

    monkeypatch.setattr(plugin, "scaffold_plugin", fake_scaffold_plugin)

    plugin.bar("init", "my-plugin", "plugins", workspace=workspace)

    assert calls["name"] == "my-plugin"
    assert calls["outdir"] == (tmp_path / "plugins").resolve()


def test_demo_init_out_path_is_workspace_relative(tmp_path: Path, monkeypatch) -> None:
    workspace = _workspace_at(tmp_path)
    (tmp_path / "nested" / "cwd").mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path / "nested" / "cwd")

    calls: dict[str, Path] = {}

    def fake_scaffold_plugin(name: str, outdir: Path) -> None:
        calls["plugin_name"] = name
        calls["plugin_outdir"] = outdir

    def fake_scaffold_demo(root: Path) -> None:
        calls["demo_root"] = root

    monkeypatch.setattr(demo, "scaffold_plugin", fake_scaffold_plugin)
    monkeypatch.setattr(demo, "scaffold_demo", fake_scaffold_demo)

    demo.handle("init", out="plugins", workspace=workspace)

    expected_out = (tmp_path / "plugins").resolve()
    assert calls["plugin_name"] == "demo"
    assert calls["plugin_outdir"] == expected_out
    assert calls["demo_root"] == (expected_out / "demo")
