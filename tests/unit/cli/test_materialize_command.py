from types import SimpleNamespace

import pytest

from datapipeline.cli.command_router import execute_command
from datapipeline.cli.parser_builder import build_parser
from datapipeline.profiles.materialize import MaterializeProfileError


def test_materialize_stream_parser() -> None:
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            "project.yaml",
            "--heartbeat-interval",
            "10",
            "stream",
            "equity.ohlcv.canonical",
            "--output",
            "equity.ohlcv.canonical.jsonl",
            "--as",
            "equity.ohlcv.materialized",
            "--overwrite",
        ]
    )

    assert args.cmd == "materialize"
    assert args.materialize_kind == "stream"
    assert args.stream_id == "equity.ohlcv.canonical"
    assert args.output == "equity.ohlcv.canonical.jsonl"
    assert args.as_stream_id == "equity.ohlcv.materialized"
    assert args.overwrite is True
    assert args.heartbeat_interval_seconds == 10


@pytest.mark.parametrize(
    ("flag", "expected"),
    [("--overwrite", True), ("--no-overwrite", False)],
)
def test_materialize_profile_parser_does_not_require_a_subcommand(
    flag: str,
    expected: bool,
) -> None:
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            "project.yaml",
            "--run",
            "adv-20",
            flag,
        ]
    )

    assert args.cmd == "materialize"
    assert args.materialize_kind is None
    assert args.run == "adv-20"
    assert args.overwrite is expected


def test_materialize_stream_can_override_parent_overwrite_flag() -> None:
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            "project.yaml",
            "--overwrite",
            "stream",
            "prices.raw",
            "--output",
            "prices.jsonl",
            "--no-overwrite",
        ]
    )

    assert args.materialize_kind == "stream"
    assert args.overwrite is False


def test_materialize_without_subcommand_dispatches_profiles(
    monkeypatch, tmp_path
) -> None:
    captured = {}

    def _handle_profiles(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(
        "datapipeline.cli.command_router.handle_materialize_profiles",
        _handle_profiles,
    )
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            str(tmp_path / "project.yaml"),
            "--run",
            "adv-20",
            "--no-overwrite",
        ]
    )

    handled = execute_command(
        args=args,
        plugin_root=None,
        workspace_context=None,
        cli_level_arg="DEBUG",
        base_level_name="INFO",
        cli_log_outputs=[],
    )

    assert handled is True
    assert captured["project"] == str(tmp_path / "project.yaml")
    assert captured["run_name"] == "adv-20"
    assert captured["overwrite"] is False
    assert captured["cli_log_level"] == "DEBUG"


def test_materialize_profile_validation_error_exits_cleanly(
    monkeypatch,
    tmp_path,
) -> None:
    def _fail(**kwargs):
        raise MaterializeProfileError("Unknown materialize profile 'missing'")

    monkeypatch.setattr(
        "datapipeline.cli.commands.materialize.run_materialize_profiles",
        _fail,
    )
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            str(tmp_path / "project.yaml"),
            "--run",
            "missing",
        ]
    )

    with pytest.raises(SystemExit) as exc_info:
        execute_command(
            args=args,
            plugin_root=None,
            workspace_context=None,
            cli_level_arg=None,
            base_level_name="INFO",
            cli_log_outputs=[],
        )

    assert exc_info.value.code == 2


@pytest.mark.parametrize("cmd", ["serve", "inspect"])
def test_cache_flags_are_removed(cmd: str) -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args([cmd, "--project", "project.yaml", "--no-cache"])


def test_materialize_command_rejects_non_jsonl_output(monkeypatch, tmp_path) -> None:
    calls = []

    def _bootstrap(project):
        calls.append(project)
        return object()

    monkeypatch.setattr("datapipeline.cli.commands.materialize.bootstrap", _bootstrap)

    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            str(tmp_path / "project.yaml"),
            "stream",
            "prices.raw",
            "--output",
            "prices.csv",
        ]
    )

    with pytest.raises(SystemExit) as exc_info:
        execute_command(
            args=args,
            plugin_root=None,
            workspace_context=None,
            cli_level_arg=None,
            base_level_name="INFO",
            cli_log_outputs=[],
        )

    assert exc_info.value.code == 2


def test_materialize_stream_rejects_profile_selection(monkeypatch) -> None:
    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            "project.yaml",
            "--run",
            "adv-20",
            "stream",
            "prices.raw",
            "--output",
            "prices.jsonl",
        ]
    )
    monkeypatch.setattr(
        "datapipeline.cli.command_router.handle_materialize_stream",
        lambda **kwargs: pytest.fail("stream handler should not run"),
    )

    with pytest.raises(SystemExit) as exc_info:
        execute_command(
            args=args,
            plugin_root=None,
            workspace_context=None,
            cli_level_arg=None,
            base_level_name="INFO",
            cli_log_outputs=[],
        )

    assert exc_info.value.code == 2


@pytest.mark.parametrize(
    ("visual_args", "expected_visuals"),
    [([], "on"), (["--visuals", "off"], "off")],
)
def test_materialize_command_uses_visual_backend_and_loads_vector_dataset(
    monkeypatch,
    tmp_path,
    visual_args,
    expected_visuals,
) -> None:
    selected = {}
    runtime = SimpleNamespace(
        registries=SimpleNamespace(stream_specs={"prices.raw": object()})
    )

    def _bootstrap(project):
        return runtime

    def _load_dataset(project, dataset_name):
        selected["dataset_name"] = dataset_name
        return object()

    def _materialize_stream_to_path(**kwargs):
        assert selected["inside_backend"] is True
        assert "visuals" not in kwargs
        assert kwargs["overwrite"] is False

        class Result:
            count = 0
            output = tmp_path / "prices.jsonl"
            metadata = tmp_path / "prices.metadata.json"
            source_config = None
            ingest_config = None

        selected["dataset"] = kwargs["dataset"]
        return Result()

    def _run_with_backend(*, visuals, runtime, level, work):
        selected["visuals"] = visuals
        selected["runtime"] = runtime
        selected["level"] = level
        selected["inside_backend"] = True
        try:
            return work()
        finally:
            selected["inside_backend"] = False

    monkeypatch.setattr("datapipeline.cli.commands.materialize.bootstrap", _bootstrap)
    monkeypatch.setattr(
        "datapipeline.cli.commands.materialize.load_dataset", _load_dataset
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.materialize.materialize_stream_to_path",
        _materialize_stream_to_path,
    )
    monkeypatch.setattr(
        "datapipeline.cli.commands.materialize.run_with_backend",
        _run_with_backend,
    )

    args = build_parser().parse_args(
        [
            "materialize",
            "--project",
            str(tmp_path / "project.yaml"),
            "stream",
            "prices.raw",
            "--output",
            "prices.jsonl",
            *visual_args,
        ]
    )

    execute_command(
        args=args,
        plugin_root=None,
        workspace_context=None,
        cli_level_arg=None,
        base_level_name="INFO",
        cli_log_outputs=[],
    )

    assert selected["dataset_name"] == "vectors"
    assert selected["dataset"] is not None
    assert selected["visuals"] == expected_visuals
    assert selected["runtime"] is runtime
    assert isinstance(selected["level"], int)
    assert selected["inside_backend"] is False
