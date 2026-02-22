from pathlib import Path
from types import SimpleNamespace

import pytest
from datapipeline.cli.commands.run import _build_cli_output_config, handle_serve


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


def test_handle_serve_exits_immediately_on_keyboard_interrupt(monkeypatch) -> None:
    profile = SimpleNamespace(
        stage=None,
        log_decision=SimpleNamespace(value=20, name="INFO"),
        runtime=SimpleNamespace(),
        limit=None,
        output=SimpleNamespace(),
        throttle_ms=None,
        visuals=SimpleNamespace(visuals="on"),
        entry=SimpleNamespace(path=Path("tasks/serve.test.yaml")),
        label="test",
        idx=1,
        total=3,
    )

    monkeypatch.setattr(
        "datapipeline.cli.commands.run.resolve_run_entries",
        lambda project_path, run_name: ([SimpleNamespace(path=Path("tasks/serve.test.yaml"))], Path(".")),
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.run._resolve_profiles",
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

    with pytest.raises(SystemExit) as exc:
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

    assert exc.value.code == 130
    assert calls["run_job"] == 1
