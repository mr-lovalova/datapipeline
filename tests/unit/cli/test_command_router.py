from pathlib import Path
from types import SimpleNamespace

import pytest

from datapipeline.cli.command_router import execute_command
from datapipeline.cli.commands import list_ as list_command
from datapipeline.cli.parser_builder import build_parser
from datapipeline.config.preview import PREVIEW_STAGES
from datapipeline.config.workspace import WorkspaceConfig, WorkspaceContext


def _execute(args, *, plugin_root=None, workspace=None) -> bool:
    return execute_command(
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

    assert _execute(build_parser().parse_args(argv)) is True

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

    assert _execute(build_parser().parse_args(argv)) is True

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

    assert (
        _execute(
            build_parser().parse_args(argv),
            plugin_root=plugin_root,
            workspace=workspace,
        )
        is True
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
            "--heartbeat-interval",
            "5",
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
    assert args.heartbeat_interval_seconds == 5


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
        "load_streams",
        lambda path: SimpleNamespace(raw={"nasa.weather": object()}),
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
