from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from datapipeline.profiles.request_builder import build_cli_output_config
from datapipeline.services.run_entries import RunEntry
from datapipeline.config.profiles import (
    BuildProfile,
    InspectProfile,
    ServeOutputConfig,
    ServeProfile,
)
from datapipeline.config.serve_resolution import _run_config_value, resolve_run_profiles
from datapipeline.config.resolution import LogOutputTarget
from datapipeline.config.tasks import OperationTask
from datapipeline.config.workspace import WorkspaceConfig, WorkspaceContext

_SERVE_OPERATION = OperationTask(
    id="serve",
    kind="runtime",
    entrypoint="core.runtime.pipeline",
)

_INSPECT_MATRIX_OPERATION = OperationTask(
    id="matrix",
    kind="runtime",
    entrypoint="core.runtime.matrix",
)

_INSPECT_COVERAGE_OPERATION = OperationTask(
    id="coverage",
    kind="runtime",
    entrypoint="core.runtime.coverage",
)


def _entry(name, config, operation):
    return RunEntry(
        name=name,
        config=config,
        target_id=operation.id,
        path=None,
    )


def test_run_config_value_ignores_model_defaults():
    cfg = ServeProfile.model_validate({"cmd": "serve", "name": "serve", "target": "serve"})

    assert _run_config_value(cfg, "observability") is None


def test_run_config_value_respects_explicit_overrides():
    cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
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
        {"cmd": "serve", "name": "serve", "target": "serve", "observability": None}
    )

    assert _run_config_value(cfg, "observability") is None


def test_run_profiles_default_build_mode_is_auto(monkeypatch, tmp_path):
    entries = [_entry(name="demo", config=None, operation=_SERVE_OPERATION)]

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
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )
    assert profiles[0].build_mode == "AUTO"


def test_operation_contract_rejects_stage_when_unsupported(monkeypatch, tmp_path):
    run_cfg = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "coverage",
            "target": "coverage",
            "build": {"mode": "AUTO"},
        }
    )
    entries = [_entry(name="coverage", config=run_cfg, operation=_INSPECT_COVERAGE_OPERATION)]

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
        match="does not support stage previews",
    ):
        resolve_run_profiles(
            project_path=tmp_path,
            run_entries=entries,
            keep=None,
            stage=1,
            limit=None,
            cli_build_mode="AUTO",
            cli_output=None,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
        )


def test_operation_contract_rejects_keep_when_unsupported(monkeypatch, tmp_path):
    run_cfg = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "coverage",
            "target": "coverage",
            "build": {"mode": "AUTO"},
        }
    )
    entries = [_entry(name="coverage", config=run_cfg, operation=_INSPECT_COVERAGE_OPERATION)]

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
        match="does not support keep filters",
    ):
        resolve_run_profiles(
            project_path=tmp_path,
            run_entries=entries,
            keep="train",
            stage=None,
            limit=None,
            cli_build_mode="AUTO",
            cli_output=None,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
        )


def test_run_profiles_cli_build_mode_overrides_profile(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "demo",
            "target": "serve",
            "build": {"mode": "OFF"},
        }
    )
    entries = [_entry(name="demo", config=run_cfg, operation=_SERVE_OPERATION)]

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
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].build_mode == "FORCE"


def test_run_profiles_use_profile_build_mode_when_cli_not_set(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "demo",
            "target": "serve",
            "build": {"mode": "OFF"},
        }
    )
    entries = [_entry(name="demo", config=run_cfg, operation=_SERVE_OPERATION)]

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
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].build_mode == "OFF"


def test_run_profiles_do_not_inherit_workspace_throttle(monkeypatch, tmp_path):
    entries = [_entry(name="demo", config=None, operation=_SERVE_OPERATION)]

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
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].throttle_ms is None


def test_run_profiles_use_builtin_visuals_defaults(monkeypatch, tmp_path):
    entries = [_entry(name="demo", config=None, operation=_SERVE_OPERATION)]

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
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].visuals.visuals == "on"


def test_run_profiles_run_visuals_override_defaults(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "demo",
            "target": "serve",
            "observability": {"visuals": "ON"},
        }
    )
    entries = [_entry(name="demo", config=run_cfg, operation=_SERVE_OPERATION)]

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
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].visuals.visuals == "on"


def test_run_profiles_resolve_log_output_precedence(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "demo",
            "target": "serve",
            "observability": {
                "logging": {"outputs": [{"transport": "stdout"}]}
            },
        }
    )
    entries = [_entry(name="demo", config=run_cfg, operation=_SERVE_OPERATION)]

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


def test_run_scoped_logs_require_serve_profile(monkeypatch, tmp_path):
    run_cfg = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "demo",
            "target": "coverage",
            "observability": {
                "logging": {
                    "outputs": [
                        {"transport": "fs", "scope": "run", "path": "logs/run.log"}
                    ]
                }
            },
        }
    )
    entries = [_entry(name="demo", config=run_cfg, operation=_INSPECT_COVERAGE_OPERATION)]

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
        match="log scope 'run' is only valid for run-scoped runtime operations",
    ):
        resolve_run_profiles(
            project_path=tmp_path / "project.yaml",
            run_entries=entries,
            keep=None,
            stage=None,
            limit=None,
            cli_build_mode="AUTO",
            cli_output=cli_output,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
        )

    serve_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
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
    serve_entries = [_entry(name="demo", config=serve_cfg, operation=_SERVE_OPERATION)]
    run_profiles = resolve_run_profiles(
        project_path=tmp_path / "project.yaml",
        run_entries=serve_entries,
        keep=None,
        stage=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=cli_output,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
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
            "cmd": "serve",
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
    entries = [_entry(name="val", config=run_cfg, operation=_SERVE_OPERATION)]

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
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
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


def test_inspect_profiles_accept_html_output_for_matrix(monkeypatch, tmp_path):
    run_cfg = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "matrix",
            "target": "matrix",
            "build": {"mode": "AUTO"},
            "output": {
                "transport": "fs",
                "format": "html",
                "directory": str(tmp_path / "inspect"),
            },
        }
    )
    entries = [_entry(name="matrix", config=run_cfg, operation=_INSPECT_MATRIX_OPERATION)]

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
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].output.format == "html"
    assert profiles[0].output.transport == "fs"


def test_inspect_profile_model_allows_html_output_for_any_target(tmp_path):
    cfg = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "coverage",
            "target": "coverage",
            "build": {"mode": "AUTO"},
            "output": {
                "transport": "fs",
                "format": "html",
                "directory": str(tmp_path / "inspect"),
            },
        }
    )
    assert cfg.output is not None
    assert cfg.output.format == "html"


def test_inspect_profiles_accept_cli_html_override_for_non_matrix(monkeypatch, tmp_path):
    run_cfg = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "coverage",
            "target": "coverage",
            "build": {"mode": "AUTO"},
        }
    )
    entries = [_entry(name="coverage", config=run_cfg, operation=_INSPECT_COVERAGE_OPERATION)]

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
        cli_output=ServeOutputConfig(
            transport="fs",
            format="html",
            directory=tmp_path / "inspect",
        ),
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )
    assert profiles[0].output.format == "html"


def test_serve_profile_model_allows_html_output(tmp_path):
    cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "serve",
            "target": "serve",
            "output": {
                "transport": "fs",
                "format": "html",
                "directory": str(tmp_path / "serve"),
            },
        }
    )
    assert cfg.output is not None
    assert cfg.output.format == "html"


def test_serve_profiles_accept_cli_txt_override(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "serve",
            "target": "serve",
            "build": {"mode": "AUTO"},
        }
    )
    entries = [_entry(name="serve", config=run_cfg, operation=_SERVE_OPERATION)]

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
        cli_output=ServeOutputConfig(
            transport="fs",
            format="txt",
            directory=tmp_path / "serve",
        ),
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )
    assert profiles[0].output.format == "txt"


def test_build_profile_rejects_runtime_output_fields():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        BuildProfile.model_validate(
            {
                "cmd": "build",
                "name": "schema",
                "target": "schema",
                "output": {
                    "transport": "fs",
                    "format": "jsonl",
                    "directory": "out",
                },
            }
        )
