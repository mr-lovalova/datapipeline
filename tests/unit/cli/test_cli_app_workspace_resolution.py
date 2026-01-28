import sys

from datapipeline.cli import app


def test_source_add_skips_dataset_resolution(monkeypatch, tmp_path):
    # Running source scaffolding should not attempt workspace dataset/project resolution.
    monkeypatch.chdir(tmp_path)

    def fail_resolution(*_args, **_kwargs):
        raise AssertionError("project/dataset resolution should be skipped for source create")

    monkeypatch.setattr(app, "_resolve_project_from_args", fail_resolution)

    captured = {}

    def fake_create_source_yaml(
        provider,
        dataset,
        loader_ep,
        loader_args,
        parser_ep,
        parser_args=None,
        root=None,
        project_yaml=None,
    ):
        captured.update(
            {
                "provider": provider,
                "dataset": dataset,
                "loader_ep": loader_ep,
                "loader_args": loader_args,
                "parser_ep": parser_ep,
                "root": root,
            }
        )

    monkeypatch.setattr(
        "datapipeline.cli.commands.source.create_source_yaml", fake_create_source_yaml
    )
    monkeypatch.setattr(
        sys, "argv", ["jerry", "source", "create", "stooq", "ohlcv_daily", "-t", "http", "-f", "json"]
    )

    app.main()

    assert captured["provider"] == "stooq"
    assert captured["dataset"] == "ohlcv_daily"
    assert captured["loader_ep"] == "core.io"
    assert captured["loader_args"]["transport"] == "http"
    assert captured["loader_args"]["format"] == "json"
    assert captured["root"] is None
    assert captured["parser_ep"] == "identity"
