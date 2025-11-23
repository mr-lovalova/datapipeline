from types import SimpleNamespace

from datapipeline.cli.commands.run_config import RunEntry
from datapipeline.config.context import _run_config_value, resolve_run_profiles
from datapipeline.config.run import RunConfig
from datapipeline.config.workspace import WorkspaceConfig, WorkspaceContext


def test_run_config_value_ignores_model_defaults():
    cfg = RunConfig.model_validate({"version": 1})

    assert _run_config_value(cfg, "visuals") is None
    assert _run_config_value(cfg, "progress") is None
    assert _run_config_value(cfg, "log_level") is None


def test_run_config_value_respects_explicit_overrides():
    cfg = RunConfig.model_validate(
        {"version": 1, "visuals": "rich", "progress": "bars", "log_level": "debug"}
    )

    assert _run_config_value(cfg, "visuals") == "RICH"
    assert _run_config_value(cfg, "progress") == "BARS"
    assert _run_config_value(cfg, "log_level") == "DEBUG"


def test_run_config_value_preserves_explicit_null():
    cfg = RunConfig.model_validate({"version": 1, "visuals": None, "progress": None})

    assert _run_config_value(cfg, "visuals") is None
    assert _run_config_value(cfg, "progress") is None


def test_run_profiles_inherit_workspace_include_targets(monkeypatch, tmp_path):
    workspace_cfg = WorkspaceConfig.model_validate({"serve": {"include_targets": True}})
    workspace = WorkspaceContext(file_path=tmp_path / "jerry.yaml", config=workspace_cfg)
    entries = [RunEntry(name="demo", config=None, path=None)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.context.iter_runtime_runs", fake_iter_runtime_runs
    )

    profiles = resolve_run_profiles(
        project_path=tmp_path,
        run_entries=entries,
        keep=None,
        stage=None,
        limit=None,
        include_targets=None,
        cli_output=None,
        cli_payload=None,
        workspace=workspace,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
        cli_progress=None,
    )

    assert profiles[0].include_targets is True
