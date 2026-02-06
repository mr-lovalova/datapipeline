import sys
from pathlib import Path

from datapipeline.cli import app
from datapipeline.config.workspace import WorkspaceConfig, WorkspaceContext


def test_source_add_skips_dataset_resolution(monkeypatch, tmp_path):
    # Running source scaffolding should not attempt workspace dataset/project resolution.
    monkeypatch.chdir(tmp_path)

    def fail_resolution(*_args, **_kwargs):
        raise AssertionError("project/dataset resolution should be skipped for source create")

    monkeypatch.setattr(app, "_resolve_project_from_args", fail_resolution)

    captured = {}

    def fake_create_source_yaml(
        provider,
        dataset,
        loader_ep,
        loader_args,
        parser_ep,
        parser_args=None,
        root=None,
        project_yaml=None,
    ):
        captured.update(
            {
                "provider": provider,
                "dataset": dataset,
                "loader_ep": loader_ep,
                "loader_args": loader_args,
                "parser_ep": parser_ep,
                "root": root,
            }
        )

    monkeypatch.setattr(
        "datapipeline.cli.commands.source.create_source_yaml", fake_create_source_yaml
    )
    monkeypatch.setattr(
        sys, "argv", ["jerry", "source", "create", "stooq", "ohlcv_daily", "-t", "http", "-f", "json"]
    )

    app.main()

    assert captured["provider"] == "stooq"
    assert captured["dataset"] == "ohlcv_daily"
    assert captured["loader_ep"] == "core.io"
    assert captured["loader_args"]["transport"] == "http"
    assert captured["loader_args"]["format"] == "json"
    assert captured["root"] is None
    assert captured["parser_ep"] == "identity"


def test_dataset_path_resolves_relative_to_workspace_root(monkeypatch, tmp_path):
    workspace_root = tmp_path
    nested = workspace_root / "nested" / "cwd"
    nested.mkdir(parents=True)
    project_file = workspace_root / "projects" / "weather" / "project.yaml"
    project_file.parent.mkdir(parents=True)
    project_file.write_text("version: 1\nname: weather\npaths: {}\n", encoding="utf-8")

    workspace = WorkspaceContext(
        file_path=workspace_root / "jerry.yaml",
        config=WorkspaceConfig.model_validate({}),
    )
    monkeypatch.chdir(nested)

    resolved = app._dataset_to_project_path("projects/weather/project.yaml", workspace)
    assert Path(resolved) == project_file.resolve()


def test_resolve_project_from_args_rejects_project_and_dataset():
    try:
        app._resolve_project_from_args("project.yaml", "alias", None)
    except SystemExit as exc:
        assert "Cannot use both --project and --dataset" in str(exc)
    else:
        raise AssertionError("Expected SystemExit when both project and dataset are set")


def test_resolve_project_from_args_uses_workspace_default_dataset(tmp_path):
    project_file = tmp_path / "datasets" / "demo" / "project.yaml"
    project_file.parent.mkdir(parents=True)
    project_file.write_text("version: 1\nname: demo\npaths: {}\n", encoding="utf-8")

    workspace = WorkspaceContext(
        file_path=tmp_path / "jerry.yaml",
        config=WorkspaceConfig.model_validate(
            {
                "datasets": {"demo": "datasets/demo/project.yaml"},
                "default_dataset": "demo",
            }
        ),
    )

    project, dataset = app._resolve_project_from_args(None, None, workspace)
    assert Path(project) == project_file.resolve()
    assert dataset == "demo"


def test_resolve_project_from_args_prefers_explicit_project_over_workspace_default(tmp_path):
    workspace = WorkspaceContext(
        file_path=tmp_path / "jerry.yaml",
        config=WorkspaceConfig.model_validate(
            {
                "datasets": {"demo": "datasets/demo/project.yaml"},
                "default_dataset": "demo",
            }
        ),
    )

    project, dataset = app._resolve_project_from_args("custom/project.yaml", None, workspace)
    assert project == "custom/project.yaml"
    assert dataset is None


def test_resolve_project_from_args_requires_selection_without_workspace_default():
    workspace = WorkspaceContext(
        file_path=Path("/tmp/jerry.yaml"),
        config=WorkspaceConfig.model_validate({}),
    )
    try:
        app._resolve_project_from_args(None, None, workspace)
    except SystemExit as exc:
        assert "No dataset/project selected" in str(exc)
    else:
        raise AssertionError("Expected SystemExit when no project selection is available")
