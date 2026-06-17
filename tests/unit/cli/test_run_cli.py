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
from datapipeline.config.split import HashSplitConfig
from datapipeline.config.tasks import ArtifactTask, OperationTask
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

_ARTIFACT_SCHEMA_TASK = ArtifactTask(
    id="schema",
    kind="artifact",
    entrypoint="core.build.schema",
    output="build/schema.json",
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


def test_serve_profile_accepts_splits_list():
    cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train", "val"],
        }
    )

    assert cfg.splits == ["train", "val"]


def test_serve_profile_rejects_splits_string():
    with pytest.raises(ValidationError, match="splits must be a list"):
        ServeProfile.model_validate(
            {
                "cmd": "serve",
                "name": "splits",
                "target": "serve",
                "splits": "train",
            }
        )


def test_serve_profile_rejects_keep_and_splits():
    with pytest.raises(ValidationError, match="both keep and splits"):
        ServeProfile.model_validate(
            {
                "cmd": "serve",
                "name": "splits",
                "target": "serve",
                "keep": "train",
                "splits": ["train"],
            }
        )


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
        preview_index=None,
        limit=None,
        cli_build_mode=None,
        cli_output=None,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )
    assert profiles[0].build_mode == "AUTO"


def test_run_profiles_resolve_splits_for_fs_output(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train", "val"],
            "output": {
                "transport": "fs",
                "format": "jsonl",
                "directory": "runs",
            },
        }
    )
    entries = [_entry(name="splits", config=run_cfg, operation=_SERVE_OPERATION)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        split = HashSplitConfig(ratios={"train": 0.8, "val": 0.2})
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config, split=split)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    profiles = resolve_run_profiles(
        project_path=tmp_path / "project.yaml",
        run_entries=entries,
        keep=None,
        preview_index=None,
        limit=None,
        cli_build_mode=None,
        cli_output=None,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )

    assert profiles[0].output.transport == "fs"
    assert profiles[0].runtime.run.splits == ["train", "val"]


def test_run_profiles_reject_splits_without_project_split(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train"],
            "output": {
                "transport": "fs",
                "format": "jsonl",
                "directory": "runs",
            },
        }
    )
    entries = [_entry(name="splits", config=run_cfg, operation=_SERVE_OPERATION)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config, split=None)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    with pytest.raises(ValueError, match="project split is not configured"):
        resolve_run_profiles(
            project_path=tmp_path / "project.yaml",
            run_entries=entries,
            keep=None,
            preview_index=None,
            limit=None,
            cli_build_mode=None,
            cli_output=None,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
        )


def test_run_profiles_reject_unknown_splits(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train", "test"],
            "output": {
                "transport": "fs",
                "format": "jsonl",
                "directory": "runs",
            },
        }
    )
    entries = [_entry(name="splits", config=run_cfg, operation=_SERVE_OPERATION)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        split = HashSplitConfig(ratios={"train": 0.8, "val": 0.2})
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config, split=split)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    with pytest.raises(ValueError, match="unknown split labels: 'test'"):
        resolve_run_profiles(
            project_path=tmp_path / "project.yaml",
            run_entries=entries,
            keep=None,
            preview_index=None,
            limit=None,
            cli_build_mode=None,
            cli_output=None,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
        )


def test_run_profiles_reject_splits_for_stdout(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train"],
        }
    )
    entries = [_entry(name="splits", config=run_cfg, operation=_SERVE_OPERATION)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        split = HashSplitConfig(ratios={"train": 1.0})
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config, split=split)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    with pytest.raises(ValueError, match="output transport is not fs"):
        resolve_run_profiles(
            project_path=tmp_path / "project.yaml",
            run_entries=entries,
            keep=None,
            preview_index=None,
            limit=None,
            cli_build_mode=None,
            cli_output=None,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
        )


def test_run_profiles_reject_splits_with_explicit_filename(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["train"],
            "output": {
                "transport": "fs",
                "format": "jsonl",
                "directory": "runs",
                "filename": "vectors",
            },
        }
    )
    entries = [_entry(name="splits", config=run_cfg, operation=_SERVE_OPERATION)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        split = HashSplitConfig(ratios={"train": 1.0})
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config, split=split)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    with pytest.raises(ValueError, match="cannot set output.filename with splits"):
        resolve_run_profiles(
            project_path=tmp_path / "project.yaml",
            run_entries=entries,
            keep=None,
            preview_index=None,
            limit=None,
            cli_build_mode=None,
            cli_output=None,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
        )


def test_run_profiles_reject_splits_with_colliding_output_filenames(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "splits",
            "target": "serve",
            "splits": ["north/west", "north_west"],
            "output": {
                "transport": "fs",
                "format": "jsonl",
                "directory": "runs",
            },
        }
    )
    entries = [_entry(name="splits", config=run_cfg, operation=_SERVE_OPERATION)]

    def fake_iter_runtime_runs(project_path, run_entries, keep):
        total = len(run_entries)
        split = HashSplitConfig(ratios={"north/west": 0.5, "north_west": 0.5})
        for idx, entry in enumerate(run_entries, start=1):
            runtime = SimpleNamespace(run=entry.config, split=split)
            yield idx, total, entry, runtime

    monkeypatch.setattr(
        "datapipeline.config.serve_resolution.iter_runtime_runs", fake_iter_runtime_runs
    )

    with pytest.raises(ValueError, match="same output filename"):
        resolve_run_profiles(
            project_path=tmp_path / "project.yaml",
            run_entries=entries,
            keep=None,
            preview_index=None,
            limit=None,
            cli_build_mode=None,
            cli_output=None,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
        )


def test_operation_options_rejects_preview_index_when_unsupported(monkeypatch, tmp_path):
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
        match="does not support preview indices",
    ):
        resolve_run_profiles(
            project_path=tmp_path,
            run_entries=entries,
            keep=None,
            preview_index=1,
            limit=None,
            cli_build_mode="AUTO",
            cli_output=None,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
        )


def test_operation_options_rejects_keep_when_unsupported(monkeypatch, tmp_path):
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
            preview_index=None,
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
        preview_index=None,
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
        preview_index=None,
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
        preview_index=None,
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
        preview_index=None,
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
        preview_index=None,
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
        preview_index=None,
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
        preview_index=None,
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


def test_execution_scoped_logs_can_be_resolved_for_inspect_profiles(monkeypatch, tmp_path):
    run_cfg = InspectProfile.model_validate(
        {
            "cmd": "inspect",
            "name": "demo",
            "target": "coverage",
            "observability": {
                "logging": {
                    "outputs": [
                        {"transport": "fs", "scope": "execution", "path": "logs/run.log"}
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

    inspect_profiles = resolve_run_profiles(
        project_path=tmp_path / "project.yaml",
        run_entries=entries,
        keep=None,
        preview_index=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=cli_output,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )
    assert inspect_profiles[0].log_output.outputs[0].scope == "execution"
    assert inspect_profiles[0].log_output.outputs[0].destination == Path("logs/run.log")

    serve_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "demo",
            "target": "serve",
            "observability": {
                "logging": {
                    "outputs": [
                        {"transport": "fs", "scope": "execution", "path": "logs/run.log"}
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
        preview_index=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=cli_output,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )
    run_profile = run_profiles[0]
    assert run_profile.output.run is not None
    assert run_profile.log_output.outputs[0].scope == "execution"
    assert run_profile.log_output.outputs[0].destination == Path("logs/run.log")


def test_execution_scoped_logs_default_to_task_specific_filename(monkeypatch, tmp_path):
    run_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "target": "serve",
            "name": "val",
            "observability": {
                "logging": {
                    "outputs": [
                        {"transport": "fs", "scope": "execution"}
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
        preview_index=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=cli_output,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
    )
    run_profile = run_profiles[0]
    assert run_profile.output.run is not None
    assert run_profile.log_output.outputs[0].scope == "execution"
    assert run_profile.log_output.outputs[0].destination is None


def test_serve_runtime_profiles_share_one_managed_run(monkeypatch, tmp_path):
    entries = [
        _entry(
            name=name,
            config=ServeProfile.model_validate({"cmd": "serve", "name": name, "target": "serve"}),
            operation=_SERVE_OPERATION,
        )
        for name in ("train", "val", "test")
    ]

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

    profiles = resolve_run_profiles(
        project_path=tmp_path / "project.yaml",
        run_entries=entries,
        keep=None,
        preview_index=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=cli_output,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
        managed_run_targets={"serve"},
    )

    run_ids = {profile.output.run.run_id for profile in profiles if profile.output.run is not None}
    dataset_dirs = {
        profile.output.run.dataset_dir for profile in profiles if profile.output.run is not None
    }

    assert len(run_ids) == 1
    assert len(dataset_dirs) == 1
    assert {profile.output.destination.name for profile in profiles} == {
        "train.jsonl",
        "val.jsonl",
        "test.jsonl",
    }


def test_shared_serve_runs_reject_explicit_output_filename(monkeypatch, tmp_path):
    entries = [
        _entry(
            name=name,
            config=ServeProfile.model_validate(
                {
                    "cmd": "serve",
                    "name": name,
                    "target": "serve",
                    "output": {
                        "transport": "fs",
                        "format": "jsonl",
                        "directory": str(tmp_path / "out"),
                        "filename": "vectors",
                    },
                }
            ),
            operation=_SERVE_OPERATION,
        )
        for name in ("train", "val")
    ]

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
        match="cannot set output.filename",
    ):
        resolve_run_profiles(
            project_path=tmp_path / "project.yaml",
            run_entries=entries,
            keep=None,
            preview_index=None,
            limit=None,
            cli_build_mode="AUTO",
            cli_output=None,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals=None,
            managed_run_targets={"serve"},
        )


def test_artifact_only_serve_profile_reuses_shared_run_logs(monkeypatch, tmp_path):
    runtime_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "train",
            "target": "serve",
                "observability": {"logging": {"outputs": [{"transport": "fs", "scope": "execution"}]}},
        }
    )
    artifact_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "schema",
            "target": "schema",
                "observability": {"logging": {"outputs": [{"transport": "fs", "scope": "execution"}]}},
        }
    )
    entries = [
        _entry(name="train", config=runtime_cfg, operation=_SERVE_OPERATION),
        _entry(name="schema", config=artifact_cfg, operation=_ARTIFACT_SCHEMA_TASK),
    ]

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

    profiles = resolve_run_profiles(
        project_path=tmp_path / "project.yaml",
        run_entries=entries,
        keep=None,
        preview_index=None,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=cli_output,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
        managed_run_targets={"serve"},
    )

    runtime_profile, artifact_profile = profiles
    assert runtime_profile.output.run is not None
    assert artifact_profile.output.run is None
    assert artifact_profile.log_output.outputs[0].scope == "execution"
    assert artifact_profile.log_output.outputs[0].destination is None


def test_artifact_only_serve_profile_ignores_node_in_mixed_serve_invocation(monkeypatch, tmp_path):
    runtime_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "train",
            "target": "serve",
        }
    )
    artifact_cfg = ServeProfile.model_validate(
        {
            "cmd": "serve",
            "name": "schema",
            "target": "schema",
        }
    )
    entries = [
        _entry(name="train", config=runtime_cfg, operation=_SERVE_OPERATION),
        _entry(name="schema", config=artifact_cfg, operation=_ARTIFACT_SCHEMA_TASK),
    ]

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
        preview_index=2,
        limit=None,
        cli_build_mode="AUTO",
        cli_output=ServeOutputConfig(
            transport="fs",
            format="jsonl",
            directory=tmp_path / "out",
        ),
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals=None,
        managed_run_targets={"serve"},
    )

    runtime_profile, artifact_profile = profiles
    assert runtime_profile.preview_index == 2
    assert artifact_profile.preview_index is None


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
        preview_index=None,
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
        preview_index=None,
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
        preview_index=None,
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
