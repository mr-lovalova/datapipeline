import sys

from datapipeline.cli import app


def test_source_add_skips_dataset_resolution(monkeypatch, tmp_path):
    # Running source scaffolding should not attempt workspace dataset/project resolution.
    monkeypatch.chdir(tmp_path)

    def fail_resolution(*_args, **_kwargs):
        raise AssertionError("project/dataset resolution should be skipped for source add")

    monkeypatch.setattr(app, "_resolve_project_from_args", fail_resolution)

    captured = {}

    def fake_create_source(provider, dataset, transport, format, root, identity):
        captured.update(
            {
                "provider": provider,
                "dataset": dataset,
                "transport": transport,
                "format": format,
                "root": root,
                "identity": identity,
            }
        )

    monkeypatch.setattr(
        "datapipeline.cli.commands.source.create_source", fake_create_source
    )
    monkeypatch.setattr(
        sys, "argv", ["jerry", "source", "add", "stooq", "ohlcv_daily", "-t", "http", "-f", "json"]
    )

    app.main()

    assert captured["provider"] == "stooq"
    assert captured["dataset"] == "ohlcv_daily"
    assert captured["transport"] == "http"
    assert captured["format"] == "json"
    assert captured["root"] is None
    assert captured["identity"] is False
