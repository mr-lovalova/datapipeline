from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.profiles.request_builder import build_cli_output_config
from datapipeline.services.runtime_entries import RunEntry
from datapipeline.config.profiles import ServeOutputConfig, ServeProfile
from datapipeline.config.serve_resolution import _run_config_value, resolve_run_profiles
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.tasks import OperationTask
from datapipeline.config.workspace import WorkspaceConfig, WorkspaceContext

_SERVE_OPERATION = OperationTask(
    id="serve",
    kind="runtime",
    entrypoint="core.runtime.pipeline",
    runtime_kind="serve",
)


def test_run_config_value_ignores_model_defaults():
    cfg = ServeProfile.model_validate({"type": "serve", "name": "serve", "target": "serve"})

    assert _run_config_value(cfg, "observability") is None


def test_run_config_value_respects_explicit_overrides():
    cfg = ServeProfile.model_validate(
        {
            "type": "serve",
            "name": "serve",
            "target": "serve",
            "observability": {
                "visuals": "off",
                "logging": {
                    "level": "debug",
                    "outputs": [{"transport": "stdout"}],
                },
            },
        }
    )

    observability = _run_config_value(cfg, "observability")
    assert observability is not None
    assert observability.visuals == "OFF"
    assert observability.logging is not None
    assert observability.logging.level == "DEBUG"
    outputs = observability.logging.outputs
    assert outputs is not None and outputs[0].transport == "STDOUT"


def test_run_config_value_preserves_explicit_null():
    cfg = ServeProfile.model_validate(
        {"type": "serve", "name": "serve", "target": "serve", "observability": None}
    )

    assert _run_config_value(cfg, "observability") is None


def test_run_profiles_require_explicit_build_mode(monkeypatch, tmp_path):
    entries = [RunEntry(name="demo", config=None, operation=_SERVE_OPERATION, path=None)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    with pytest.raises(
        ValueError,
        match="must define build.mode or pass --build-mode",
    ):
        resolve_run_profiles(
            project_path=tmp_path,
            run_entries=entries,
            keep=None,
            stage=None,
            limit=None,
            cli_build_mode=None,
            cli_output=None,
            workspace=None,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
        )


def test_run_profiles_cli_build_mode_overrides_profile(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "type": "serve",
            "name": "demo",
            "target": "serve",
            "build": {"mode": "OFF"},
        }
    )
    entries = [RunEntry(name="demo", config=run_cfg, operation=_SERVE_OPERATION, path=None)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    profiles = resolve_run_profiles(
        project_path=tmp_path,
        run_entries=entries,
        keep=None,
        stage=None,
        limit=None,
        cli_build_mode="FORCE",
        cli_output=None,
        workspace=None,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].build_mode == "FORCE"


def test_run_profiles_use_profile_build_mode_when_cli_not_set(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "type": "serve",
            "name": "demo",
            "target": "serve",
            "build": {"mode": "OFF"},
        }
    )
    entries = [RunEntry(name="demo", config=run_cfg, operation=_SERVE_OPERATION, path=None)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    profiles = resolve_run_profiles(
        project_path=tmp_path,
        run_entries=entries,
        keep=None,
        stage=None,
        limit=None,
        cli_build_mode=None,
        cli_output=None,
        workspace=None,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].build_mode == "OFF"


def test_run_profiles_do_not_inherit_workspace_throttle(monkeypatch, tmp_path):
    workspace_cfg = WorkspaceConfig.model_validate({})
    workspace = WorkspaceContext(file_path=tmp_path / "jerry.yaml", config=workspace_cfg)
    entries = [RunEntry(name="demo", config=None, operation=_SERVE_OPERATION, path=None)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    profiles = resolve_run_profiles(
        project_path=tmp_path,
        run_entries=entries,
        keep=None,
        stage=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=None,
        workspace=workspace,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].throttle_ms is None


def test_run_profiles_use_shared_visuals_defaults(monkeypatch, tmp_path):
    workspace_cfg = WorkspaceConfig.model_validate(
        {
            "shared": {"observability": {"visuals": "OFF"}},
        }
    )
    workspace = WorkspaceContext(file_path=tmp_path / "jerry.yaml", config=workspace_cfg)
    entries = [RunEntry(name="demo", config=None, operation=_SERVE_OPERATION, path=None)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    profiles = resolve_run_profiles(
        project_path=tmp_path,
        run_entries=entries,
        keep=None,
        stage=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=None,
        workspace=workspace,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].visuals.visuals == "off"


def test_run_profiles_run_visuals_override_shared_defaults(monkeypatch, tmp_path):
    workspace_cfg = WorkspaceConfig.model_validate(
        {
            "shared": {"observability": {"visuals": "OFF"}},
        }
    )
    workspace = WorkspaceContext(file_path=tmp_path / "jerry.yaml", config=workspace_cfg)
    run_cfg = ServeProfile.model_validate(
        {
            "type": "serve",
            "name": "demo",
            "target": "serve",
            "observability": {"visuals": "ON"},
        }
    )
    entries = [RunEntry(name="demo", config=run_cfg, operation=_SERVE_OPERATION, path=None)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    profiles = resolve_run_profiles(
        project_path=tmp_path,
        run_entries=entries,
        keep=None,
        stage=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=None,
        workspace=workspace,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].visuals.visuals == "on"


def test_run_profiles_resolve_log_output_precedence(monkeypatch, tmp_path):
    workspace_cfg = WorkspaceConfig.model_validate(
        {
            "shared": {
                "observability": {
                    "logging": {"outputs": [{"transport": "stderr"}]}
                }
            },
        }
    )
    workspace = WorkspaceContext(file_path=tmp_path / "jerry.yaml", config=workspace_cfg)
    run_cfg = ServeProfile.model_validate(
        {
            "type": "serve",
            "name": "demo",
            "target": "serve",
            "observability": {
                "logging": {"outputs": [{"transport": "stdout"}]}
            },
        }
    )
    entries = [RunEntry(name="demo", config=run_cfg, operation=_SERVE_OPERATION, path=None)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    profiles = resolve_run_profiles(
        project_path=tmp_path / "project.yaml",
        run_entries=entries,
        keep=None,
        stage=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=None,
        workspace=workspace,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )
    assert profiles[0].log_output.outputs[0].transport == "stdout"
    assert profiles[0].log_output.outputs[0].destination is None

    profiles_cli = resolve_run_profiles(
        project_path=tmp_path / "project.yaml",
        run_entries=entries,
        keep=None,
        stage=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=None,
        workspace=workspace,
        cli_log_level=None,
        cli_log_outputs=[LogOutputTarget(
            transport="fs",
            destination=tmp_path / "logs" / "cli.log",
        )],
        base_log_level="INFO",
        cli_visuals=None,
    )
    assert profiles_cli[0].log_output.outputs[0].transport == "fs"
    assert profiles_cli[0].log_output.outputs[0].destination == (tmp_path / "logs" / "cli.log")


def test_run_scoped_logs_materialize_only_for_create_run(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "type": "serve",
            "name": "demo",
            "target": "serve",
            "observability": {
                "logging": {
                    "outputs": [
                        {"transport": "fs", "scope": "run", "path": "logs/run.log"}
                    ]
                }
            },
        }
    )
    entries = [RunEntry(name="demo", config=run_cfg, operation=_SERVE_OPERATION, path=None)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    cli_output = ServeOutputConfig(
        transport="fs",
        format="jsonl",
        directory=tmp_path / "out",
    )

    with pytest.raises(
        ValueError,
        match="log scope 'run' is only valid for serve execution",
    ):
        resolve_run_profiles(
            project_path=tmp_path / "project.yaml",
            run_entries=entries,
            keep=None,
            stage=None,
            limit=None,
            cli_build_mode="AUTO",
            cli_output=cli_output,
            workspace=None,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
            create_run=False,
        )

    run_profiles = resolve_run_profiles(
        project_path=tmp_path / "project.yaml",
        run_entries=entries,
        keep=None,
        stage=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=cli_output,
        workspace=None,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
        create_run=True,
    )
    run_profile = run_profiles[0]
    assert run_profile.output.run is not None
    assert run_profile.log_output.outputs[0].scope == "global"
    assert run_profile.log_output.outputs[0].destination == (
        run_profile.output.run.dataset_dir / "logs" / "run.log"
    )


def test_run_scoped_logs_default_to_task_specific_filename(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "type": "serve",
            "target": "serve",
            "name": "val",
            "observability": {
                "logging": {
                    "outputs": [
                        {"transport": "fs", "scope": "run"}
                    ]
                }
            },
        }
    )
    entries = [RunEntry(name="val", config=run_cfg, operation=_SERVE_OPERATION, path=None)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    cli_output = ServeOutputConfig(
        transport="fs",
        format="jsonl",
        directory=tmp_path / "out",
    )

    run_profiles = resolve_run_profiles(
        project_path=tmp_path / "project.yaml",
        run_entries=entries,
        keep=None,
        stage=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=cli_output,
        workspace=None,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
        create_run=True,
    )
    run_profile = run_profiles[0]
    assert run_profile.output.run is not None
    assert run_profile.log_output.outputs[0].scope == "global"
    assert run_profile.log_output.outputs[0].destination == (
        run_profile.output.run.dataset_dir / "logs" / "serve.val.log"
    )


def test_cli_output_directory_resolves_relative_to_workspace(tmp_path):
    workspace_cfg = WorkspaceConfig.model_validate({})
    workspace = WorkspaceContext(file_path=tmp_path / "jerry.yaml", config=workspace_cfg)

    cfg = build_cli_output_config(
        "fs",
        "jsonl",
        ".",
        workspace=workspace,
    )

    assert cfg is not None
    assert cfg.directory == tmp_path.resolve()
