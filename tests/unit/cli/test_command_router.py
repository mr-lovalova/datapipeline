from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.cli.command_router import execute_command
from datapipeline.cli.commands import list_ as list_command
from datapipeline.cli.parser_builder import build_parser
from datapipeline.cli.workspace import WorkspaceContext
from datapipeline.config.preview import PREVIEW_STAGES
from datapipeline.config.workspace import WorkspaceConfig


def _execute(args, *, plugin_root=None, workspace=None) -> None:
    execute_command(
        args=args,
        plugin_root=plugin_root,
        workspace_context=workspace,
        cli_level_arg=None,
        base_level_name="INFO",
        cli_log_outputs=[],
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["plugin", "init", "weather-plugin"],
        ["plugin", "init", "--name", "weather-plugin"],
        ["plugin", "init", "-n", "weather-plugin"],
    ],
)
def test_plugin_name_forms_dispatch_the_same_value(monkeypatch, argv) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "datapipeline.cli.command_router.handle_plugin",
        lambda **kwargs: captured.update(kwargs),
    )

    _execute(build_parser().parse_args(argv))

    assert captured["subcmd"] == "init"
    assert captured["name"] == "weather-plugin"


@pytest.mark.parametrize(
    "argv",
    [
        ["domain", "create", "weather"],
        ["domain", "create", "--name", "weather"],
        ["domain", "create", "-n", "weather"],
    ],
)
def test_domain_name_forms_dispatch_the_same_value(monkeypatch, argv) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "datapipeline.cli.command_router.handle_domain",
        lambda **kwargs: captured.update(kwargs),
    )

    _execute(build_parser().parse_args(argv))

    assert captured["subcmd"] == "create"
    assert captured["domain"] == "weather"


@pytest.mark.parametrize(
    "argv",
    [
        ["plugin", "init", "one", "--name", "two"],
        ["domain", "create", "one", "--name", "two"],
    ],
)
def test_positional_and_option_names_are_mutually_exclusive(argv) -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(argv)

    assert exc.value.code == 2


@pytest.mark.parametrize(
    ("argv", "subcmd"),
    [
        (["list", "domains"], "domains"),
        (["domain", "list"], "domains"),
        (["source", "list"], "sources"),
    ],
)
def test_list_routes_forward_plugin_and_workspace(
    monkeypatch,
    tmp_path,
    argv,
    subcmd,
) -> None:
    plugin_root = tmp_path / "plugin"
    workspace = WorkspaceContext(
        file_path=tmp_path / "jerry.yaml",
        config=WorkspaceConfig.model_validate({}),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "datapipeline.cli.command_router.handle_list",
        lambda **kwargs: captured.update(kwargs),
    )

    _execute(
        build_parser().parse_args(argv),
        plugin_root=plugin_root,
        workspace=workspace,
    )

    assert captured == {
        "subcmd": subcmd,
        "plugin_root": plugin_root,
        "workspace": workspace,
    }


@pytest.mark.parametrize(
    "argv",
    [
        [
            "--log-level",
            "DEBUG",
            "--log-output",
            "stdout",
            "serve",
            "--project",
            "project.yaml",
        ],
        [
            "serve",
            "--log-level",
            "DEBUG",
            "--log-output",
            "stdout",
            "--heartbeat-interval",
            "5",
            "--project",
            "project.yaml",
        ],
    ],
)
def test_common_options_survive_before_or_after_command(argv) -> None:
    args = build_parser().parse_args(argv)

    assert args.log_level == "DEBUG"
    assert args.log_output == ["stdout"]


@pytest.mark.parametrize("command", ["serve", "build", "inspect", "materialize"])
def test_execution_commands_accept_observability_flags(command) -> None:
    args = build_parser().parse_args(
        [command, "--visuals", "off", "--heartbeat-interval", "5"]
    )

    assert args.visuals == "off"
    assert args.heartbeat_interval_seconds == 5


def test_heartbeat_is_an_execution_command_option() -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["--heartbeat-interval", "5", "serve"])

    assert exc.value.code == 2


@pytest.mark.parametrize(
    "argv",
    [
        ["clean"],
        ["demo", "init"],
        ["list", "domains"],
        ["source", "list"],
    ],
)
def test_non_execution_commands_reject_heartbeat(argv) -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args([*argv, "--heartbeat-interval", "5"])

    assert exc.value.code == 2


@pytest.mark.parametrize("value", ["-1", "nan", "inf", "-inf"])
def test_heartbeat_rejects_invalid_values(value) -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["serve", "--heartbeat-interval", value])

    assert exc.value.code == 2


@pytest.mark.parametrize("command", ["serve", "inspect"])
@pytest.mark.parametrize("value", ["0", "-1", "1.5", "many"])
def test_runtime_limit_must_be_a_positive_integer(command, value) -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args([command, "--limit", value])

    assert exc.value.code == 2


def test_runtime_limit_accepts_a_positive_integer() -> None:
    args = build_parser().parse_args(["serve", "--limit", "1"])

    assert args.limit == 1


@pytest.mark.parametrize("command", ["serve", "build", "inspect", "materialize"])
def test_profile_flag_selects_a_profile(command) -> None:
    args = build_parser().parse_args([command, "--profile", "disabled-profile"])

    assert args.profile == "disabled-profile"


def test_profile_help_explains_explicit_disabled_selection(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["serve", "--help"])

    assert exc.value.code == 0
    help_text = " ".join(capsys.readouterr().out.split())
    assert "explicitly selected disabled profiles still run" in help_text


@pytest.mark.parametrize("preview", PREVIEW_STAGES)
def test_serve_parser_accepts_semantic_preview_stages(preview) -> None:
    args = build_parser().parse_args(["serve", "--preview", preview])

    assert args.preview == preview


def test_serve_parser_rejects_numeric_preview(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(["serve", "--preview", "3"])

    assert exc.value.code == 2


def test_source_listing_uses_workspace_project_without_python_package(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    project_yaml = tmp_path / "dataset" / "project.yaml"
    monkeypatch.setattr(
        list_command,
        "resolve_default_project_yaml",
        lambda workspace: project_yaml,
    )
    monkeypatch.setattr(
        list_command,
        "load_project",
        lambda path: path,
    )
    monkeypatch.setattr(
        list_command,
        "load_streams",
        lambda project: SimpleNamespace(sources={"nasa.weather": object()}),
    )
    monkeypatch.setattr(
        list_command,
        "pkg_root",
        lambda root: (_ for _ in ()).throw(
            AssertionError("workspace source listing must not require a Python package")
        ),
    )

    list_command.handle("sources", workspace=object())

    assert capsys.readouterr().out.strip() == "nasa.weather"


@pytest.mark.parametrize(
    ("subcmd", "function_name"),
    [
        ("domains", "list_domains"),
        ("dtos", "list_dtos"),
        ("parsers", "list_parsers"),
        ("mappers", "list_mappers"),
        ("combiners", "list_combiners"),
        ("loaders", "list_loaders"),
    ],
)
def test_plugin_discovery_lists_use_configured_root(
    monkeypatch,
    tmp_path,
    subcmd,
    function_name,
) -> None:
    plugin_root = tmp_path / "plugin"
    seen: list[Path | None] = []
    monkeypatch.setattr(
        list_command,
        function_name,
        lambda *, root: seen.append(root) or {},
    )

    list_command.handle(subcmd, plugin_root=plugin_root)

    assert seen == [plugin_root]
