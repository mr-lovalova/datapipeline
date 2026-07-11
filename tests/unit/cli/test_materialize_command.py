import pytest

from datapipeline.cli.command_router import execute_command
from datapipeline.cli.parser_builder import build_parser


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
            "--force",
        ]
    )

    assert args.cmd == "materialize"
    assert args.materialize_kind == "stream"
    assert args.stream_id == "equity.ohlcv.canonical"
    assert args.output == "equity.ohlcv.canonical.jsonl"
    assert args.as_stream_id == "equity.ohlcv.materialized"
    assert args.force is True
    assert args.heartbeat_interval_seconds == 10


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

    with pytest.raises(ValueError, match=".jsonl"):
        execute_command(
            args=args,
            plugin_root=None,
            workspace_context=None,
            cli_level_arg=None,
            base_level_name="INFO",
            cli_log_outputs=[],
        )


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
    runtime = object()

    def _bootstrap(project):
        return runtime

    def _load_dataset(project, dataset_name):
        selected["dataset_name"] = dataset_name
        return object()

    def _materialize_stream_to_path(**kwargs):
        assert selected["inside_backend"] is True
        assert "visuals" not in kwargs

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
