from types import SimpleNamespace

import pytest

from datapipeline.cli.command_router import execute_command
from datapipeline.cli.parser_builder import build_parser
from datapipeline.profiles.materialize import MaterializeProfileError


def _execute(args, workspace=None) -> bool:
    return execute_command(
        args=args,
        plugin_root=None,
        workspace_context=workspace,
        cli_level_arg="DEBUG",
        base_level_name="INFO",
        cli_log_outputs=[],
    )


def test_materialize_parser_accepts_profile_overrides() -> None:
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            "project.yaml",
            "--run",
            "adv-20",
            "--output",
            "adv-20.jsonl",
            "--overwrite",
            "--visuals",
            "off",
            "--heartbeat-interval",
            "10",
        ]
    )

    assert args.cmd == "materialize"
    assert args.run == "adv-20"
    assert args.output == "adv-20.jsonl"
    assert args.overwrite is True
    assert args.visuals == "off"
    assert args.heartbeat_interval_seconds == 10


def test_materialize_dispatches_one_profile_execution_path(monkeypatch) -> None:
    captured = {}
    monkeypatch.setattr(
        "datapipeline.cli.command_router.handle_materialize",
        lambda **kwargs: captured.update(kwargs),
    )
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            "project.yaml",
            "--run",
            "adv-20",
            "--output",
            "adv-20.jsonl",
            "--no-overwrite",
        ]
    )

    assert _execute(args) is True
    assert captured == {
        "project": "project.yaml",
        "run_name": "adv-20",
        "output": "adv-20.jsonl",
        "overwrite": False,
        "visuals": None,
        "heartbeat_interval_seconds": None,
        "cli_log_level": "DEBUG",
        "cli_log_outputs": [],
        "base_log_level": "INFO",
        "workspace": None,
    }


def test_materialize_output_override_requires_run(monkeypatch) -> None:
    monkeypatch.setattr(
        "datapipeline.cli.commands.materialize.run_materialize_profiles",
        lambda **kwargs: pytest.fail("profiles should not run"),
    )
    args = build_parser().parse_args(
        ["materialize", "--project", "project.yaml", "--output", "out.jsonl"]
    )

    with pytest.raises(SystemExit) as exc_info:
        _execute(args)

    assert exc_info.value.code == 2


def test_materialize_resolves_profile_output_from_workspace(
    monkeypatch, tmp_path
) -> None:
    captured = {}
    monkeypatch.setattr(
        "datapipeline.cli.commands.materialize.run_materialize_profiles",
        lambda **kwargs: captured.update(kwargs) or [object()],
    )
    workspace = SimpleNamespace(root=tmp_path)
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            "project.yaml",
            "--run",
            "adv-20",
            "--output",
            "outputs/adv-20.jsonl",
        ]
    )

    assert _execute(args, workspace) is True
    assert captured["cli_output"] == (tmp_path / "outputs/adv-20.jsonl").resolve()
    assert captured["run_name"] == "adv-20"


def test_materialize_rejects_non_jsonl_output(monkeypatch) -> None:
    monkeypatch.setattr(
        "datapipeline.cli.commands.materialize.run_materialize_profiles",
        lambda **kwargs: pytest.fail("profiles should not run"),
    )
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            "project.yaml",
            "--run",
            "adv-20",
            "--output",
            "adv-20.csv",
        ]
    )

    with pytest.raises(SystemExit) as exc_info:
        _execute(args)

    assert exc_info.value.code == 2


def test_materialize_allows_global_overrides_without_run(monkeypatch) -> None:
    captured = {}
    monkeypatch.setattr(
        "datapipeline.cli.commands.materialize.run_materialize_profiles",
        lambda **kwargs: captured.update(kwargs) or [object()],
    )
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            "project.yaml",
            "--overwrite",
            "--visuals",
            "off",
        ]
    )

    assert _execute(args) is True
    assert captured["run_name"] is None
    assert captured["cli_output"] is None
    assert captured["overwrite"] is True
    assert captured["cli_visuals"] == "off"


def test_materialize_profile_validation_error_exits_cleanly(monkeypatch) -> None:
    def fail(**kwargs):
        raise MaterializeProfileError("Unknown materialize profile 'missing'")

    monkeypatch.setattr(
        "datapipeline.cli.commands.materialize.run_materialize_profiles",
        fail,
    )
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            "project.yaml",
            "--run",
            "missing",
        ]
    )

    with pytest.raises(SystemExit) as exc_info:
        _execute(args)

    assert exc_info.value.code == 2
