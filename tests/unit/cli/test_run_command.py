from pathlib import Path
from types import SimpleNamespace
import logging

import pytest
from datapipeline.cli.commands.run import (
    _build_cli_output_config,
    _log_profile_start_debug,
    handle_serve,
)
from datapipeline.config.resolution import LogOutputSettings, LogOutputTarget


def test_build_cli_output_config_fs_requires_directory() -> None:
    with pytest.raises(SystemExit) as exc:
        _build_cli_output_config(
            transport="fs",
            fmt="jsonl",
            directory=None,
        )
    assert exc.value.code == 2


def test_build_cli_output_config_stdout_rejects_directory() -> None:
    with pytest.raises(SystemExit) as exc:
        _build_cli_output_config(
            transport="stdout",
            fmt="jsonl",
            directory="out",
        )
    assert exc.value.code == 2


def test_build_cli_output_config_rejects_stdout_csv() -> None:
    with pytest.raises(SystemExit) as exc:
        _build_cli_output_config(
            transport="stdout",
            fmt="csv",
            directory=None,
            view="flat",
        )
    assert exc.value.code == 2


def test_build_cli_output_config_fs_populates_output() -> None:
    config = _build_cli_output_config(
        transport="fs",
        fmt="jsonl",
        directory="artifacts",
    )
    assert config is not None
    assert config.transport == "fs"
    assert config.format == "jsonl"
    assert config.view is None
    assert config.encoding == "utf-8"
    assert config.directory == Path("artifacts").resolve()


def test_build_cli_output_config_honors_view() -> None:
    config = _build_cli_output_config(
        transport="stdout",
        fmt="jsonl",
        directory=None,
        view="values",
    )
    assert config is not None
    assert config.view == "values"


def test_build_cli_output_config_rejects_non_flat_csv_view() -> None:
    with pytest.raises(SystemExit) as exc:
        _build_cli_output_config(
            transport="fs",
            fmt="csv",
            directory="out",
            view="raw",
        )
    assert exc.value.code == 2


def test_build_cli_output_config_honors_encoding_for_fs() -> None:
    config = _build_cli_output_config(
        transport="fs",
        fmt="csv",
        directory="out",
        output_encoding="utf-8-sig",
        view="flat",
    )
    assert config is not None
    assert config.encoding == "utf-8-sig"


def test_build_cli_output_config_rejects_encoding_for_stdout() -> None:
    with pytest.raises(SystemExit) as exc:
        _build_cli_output_config(
            transport="stdout",
            fmt="jsonl",
            directory=None,
            output_encoding="utf-8",
        )
    assert exc.value.code == 2


def test_build_cli_output_config_rejects_unknown_encoding() -> None:
    with pytest.raises(SystemExit) as exc:
        _build_cli_output_config(
            transport="fs",
            fmt="jsonl",
            directory="out",
            output_encoding="definitely-not-a-codec",
        )
    assert exc.value.code == 2


def test_handle_serve_propagates_keyboard_interrupt(monkeypatch) -> None:
    profile = SimpleNamespace(
        stage=None,
        log_decision=SimpleNamespace(value=20, name="INFO"),
        runtime=SimpleNamespace(),
        limit=None,
        output=SimpleNamespace(),
        throttle_ms=None,
        visuals=SimpleNamespace(visuals="on"),
        log_output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
        entry=SimpleNamespace(path=Path("tasks/profiles/serve.test.yaml")),
        label="test",
        idx=1,
        total=3,
    )

    monkeypatch.setattr(
        "datapipeline.cli.commands.run.resolve_run_entries",
        lambda project_path, run_name: ([SimpleNamespace(path=Path("tasks/profiles/serve.test.yaml"))], Path(".")),
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.run._resolve_profiles_or_exit",
        lambda **kwargs: [profile],
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.run.load_dataset",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.run._load_dataset_for_profile",
        lambda **kwargs: object(),
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.run._entry_sections",
        lambda run_root, entry: ("Run Tasks",),
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.run.ensure_stage_artifacts",
        lambda *args, **kwargs: None,
    )

    calls = {"run_job": 0}

    def _interrupting_run_job(**kwargs):
        calls["run_job"] += 1
        raise KeyboardInterrupt()

    monkeypatch.setattr(
        "datapipeline.cli.commands.run.run_job",
        _interrupting_run_job,
    )

    with pytest.raises(KeyboardInterrupt):
        handle_serve(
            project="project.yaml",
            limit=None,
            keep=None,
            run_name=None,
            stage=None,
            output_transport=None,
            output_format=None,
            output_directory=None,
            output_encoding=None,
            output_view=None,
            skip_build=True,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals="on",
            workspace=None,
        )

    assert calls["run_job"] == 1


def test_handle_serve_skip_build_resolves_run_scoped_logs(monkeypatch) -> None:
    unresolved_profile = SimpleNamespace(
        stage=None,
        log_decision=SimpleNamespace(value=20, name="INFO"),
        runtime=SimpleNamespace(),
        limit=None,
        output=SimpleNamespace(run=None, transport="stdout", format="jsonl", view="raw", encoding=None, destination=None),
        throttle_ms=None,
        visuals=SimpleNamespace(visuals="on"),
        log_output=LogOutputSettings(outputs=(LogOutputTarget(transport="fs", scope="run"),)),
        entry=SimpleNamespace(path=Path("tasks/profiles/serve.test.yaml")),
        label="test",
        idx=1,
        total=1,
    )
    resolved_profile = SimpleNamespace(
        stage=None,
        log_decision=SimpleNamespace(value=20, name="INFO"),
        runtime=SimpleNamespace(),
        limit=None,
        output=SimpleNamespace(run=None, transport="stdout", format="jsonl", view="raw", encoding=None, destination=None),
        throttle_ms=None,
        visuals=SimpleNamespace(visuals="on"),
        log_output=LogOutputSettings(
            outputs=(LogOutputTarget(transport="fs", destination=Path("/tmp/serve.test.log")),)
        ),
        entry=SimpleNamespace(path=Path("tasks/profiles/serve.test.yaml")),
        label="test",
        idx=1,
        total=1,
    )

    monkeypatch.setattr(
        "datapipeline.cli.commands.run.resolve_run_entries",
        lambda project_path, run_name: ([SimpleNamespace(path=Path("tasks/profiles/serve.test.yaml"))], Path(".")),
    )
    create_run_flags: list[bool] = []

    def _fake_resolve_profiles(**kwargs):
        create_run = bool(kwargs.get("create_run"))
        create_run_flags.append(create_run)
        return [resolved_profile] if create_run else [unresolved_profile]

    monkeypatch.setattr("datapipeline.cli.commands.run._resolve_profiles_or_exit", _fake_resolve_profiles)
    monkeypatch.setattr("datapipeline.cli.commands.run.load_dataset", lambda *args, **kwargs: object())
    monkeypatch.setattr("datapipeline.cli.commands.run._load_dataset_for_profile", lambda **kwargs: object())
    monkeypatch.setattr("datapipeline.cli.commands.run._entry_sections", lambda run_root, entry: ("Run Tasks",))
    monkeypatch.setattr("datapipeline.cli.commands.run.ensure_stage_artifacts", lambda *args, **kwargs: None)

    configured_outputs: list[LogOutputSettings] = []
    monkeypatch.setattr(
        "datapipeline.cli.commands.run.configure_root_logging",
        lambda *, level, output: configured_outputs.append(output),
    )
    monkeypatch.setattr("datapipeline.cli.commands.run.run_job", lambda **kwargs: None)

    handle_serve(
        project="project.yaml",
        limit=None,
        keep=None,
        run_name=None,
        stage=None,
        output_transport=None,
        output_format=None,
        output_directory=None,
        output_encoding=None,
        output_view=None,
        skip_build=True,
        cli_log_level=None,
        base_log_level="INFO",
        cli_visuals="on",
        workspace=None,
    )

    assert create_run_flags == [True]
    assert configured_outputs
    assert configured_outputs[0].outputs[0].destination == Path("/tmp/serve.test.log")


def test_log_profile_start_debug_emits_execution_message(monkeypatch, caplog) -> None:
    profile = SimpleNamespace(
        idx=1,
        total=1,
        label="train",
        stage=None,
        limit=None,
        throttle_ms=None,
        log_decision=SimpleNamespace(name="DEBUG", value=10),
        visuals=SimpleNamespace(visuals="on"),
        log_output=LogOutputSettings(outputs=(LogOutputTarget(transport="stderr"),)),
        output=SimpleNamespace(
            transport="fs",
            format="jsonl",
            view="raw",
            encoding="utf-8",
            destination=Path("/tmp/train.jsonl"),
        ),
        entry=SimpleNamespace(name="train", path=Path("tasks/profiles/serve.train.yaml"), config=None),
    )

    captured: list[tuple[str, int, str | None]] = []
    monkeypatch.setattr(
        "datapipeline.cli.commands.run.emit_execution_message",
        lambda message, level, logger, message_kind=None: captured.append((message, level, message_kind)),
    )

    with caplog.at_level(logging.DEBUG, logger="datapipeline.cli.commands.run"):
        _log_profile_start_debug(profile, profile_path=Path("/tmp/serve.train.profile.json"))

    assert captured
    message, level, message_kind = captured[0]
    assert message.startswith("Run profile start (1/1) label=train")
    assert "profile=/tmp/serve.train.profile.json" in message
    assert level == 10
    assert message_kind == "task_config"


def test_handle_serve_skips_when_no_enabled_profiles(monkeypatch, caplog) -> None:
    monkeypatch.setattr(
        "datapipeline.cli.commands.run.resolve_run_entries",
        lambda project_path, run_name: ([], Path(".")),
    )

    called = {"resolve_profiles": 0}

    def _should_not_run(**kwargs):
        called["resolve_profiles"] += 1
        return []

    monkeypatch.setattr(
        "datapipeline.cli.commands.run._resolve_profiles_or_exit",
        _should_not_run,
    )

    with caplog.at_level(logging.INFO, logger="datapipeline.cli.commands.run"):
        handle_serve(
            project="project.yaml",
            limit=None,
            keep=None,
            run_name=None,
            stage=None,
            output_transport=None,
            output_format=None,
            output_directory=None,
            output_encoding=None,
            output_view=None,
            skip_build=False,
            cli_log_level=None,
            base_log_level="INFO",
            cli_visuals="on",
            workspace=None,
        )

    assert called["resolve_profiles"] == 0
    assert "No enabled serve profiles; skipping serve." in caplog.text
