from pathlib import Path

import pytest

from datapipeline.config.profiles import ServeOutputConfig
from datapipeline.io.output import resolve_output_target


def test_resolve_output_target_uses_directory_and_run_name(tmp_path):
    default_dir = tmp_path / "outputs"
    default_dir.mkdir()
    cfg = ServeOutputConfig(transport="fs", format="jsonl", directory=default_dir)

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train",
    )

    assert target.transport == "fs"
    assert target.format == "jsonl"
    assert target.view == "raw"
    assert target.encoding == "utf-8"
    assert target.destination == (default_dir / "train" / "train.jsonl").resolve()


def test_resolve_output_target_honors_custom_filename(tmp_path):
    base_dir = tmp_path / "outputs"
    base_dir.mkdir()
    cfg = ServeOutputConfig(transport="fs", format="jsonl", directory=base_dir, filename="custom")

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train",
    )

    assert target.destination == (base_dir / "train" / "custom.jsonl").resolve()
    assert target.encoding == "utf-8"


def test_resolve_output_target_sanitizes_derived_filename_stem(tmp_path):
    base_dir = tmp_path / "outputs"
    base_dir.mkdir()
    cfg = ServeOutputConfig(transport="fs", format="jsonl", directory=base_dir)

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train/val",
    )

    assert target.destination == (base_dir / "train_val" / "train_val.jsonl").resolve()


def test_resolve_output_target_honors_view(tmp_path):
    out_dir = tmp_path / "outputs"
    out_dir.mkdir()
    cfg = ServeOutputConfig(
        transport="fs",
        format="jsonl",
        view="values",
        directory=out_dir,
    )

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train",
    )

    assert target.view == "values"


def test_resolve_output_target_honors_encoding(tmp_path):
    out_dir = tmp_path / "outputs"
    out_dir.mkdir()
    cfg = ServeOutputConfig(
        transport="fs",
        format="jsonl",
        encoding="utf-8-sig",
        directory=out_dir,
    )

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="train",
    )

    assert target.encoding == "utf-8-sig"


def test_stdout_jsonl_defaults_to_raw_view():
    target = resolve_output_target(
        cli_output=ServeOutputConfig(transport="stdout", format="jsonl"),
        config_output=None,
        default=None,
        base_path=Path("."),
        run_name="train",
    )
    assert target.view == "raw"


def test_stdout_txt_is_supported():
    target = resolve_output_target(
        cli_output=ServeOutputConfig(transport="stdout", format="txt"),
        config_output=None,
        default=None,
        base_path=Path("."),
        run_name="report",
    )
    assert target.transport == "stdout"
    assert target.format == "txt"


def test_resolve_output_target_html_uses_html_suffix(tmp_path):
    out_dir = tmp_path / "outputs"
    out_dir.mkdir()
    cfg = ServeOutputConfig(
        transport="fs",
        format="html",
        directory=out_dir,
    )

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="matrix",
    )

    assert target.destination == (out_dir / "matrix" / "matrix.html").resolve()


def test_resolve_output_target_txt_uses_txt_suffix(tmp_path):
    out_dir = tmp_path / "outputs"
    out_dir.mkdir()
    cfg = ServeOutputConfig(
        transport="fs",
        format="txt",
        directory=out_dir,
    )

    target = resolve_output_target(
        cli_output=None,
        config_output=cfg,
        default=None,
        base_path=Path("."),
        run_name="coverage",
    )

    assert target.destination == (out_dir / "coverage" / "coverage.txt").resolve()
    assert target.encoding == "utf-8"


def test_html_output_rejects_view_and_encoding(tmp_path):
    out_dir = tmp_path / "outputs"
    out_dir.mkdir()
    with pytest.raises(ValueError, match="html output does not support view"):
        ServeOutputConfig(
            transport="fs",
            format="html",
            view="raw",
            directory=out_dir,
        )
    with pytest.raises(ValueError, match="html output does not support encoding"):
        ServeOutputConfig(
            transport="fs",
            format="html",
            encoding="utf-8",
            directory=out_dir,
        )


def test_txt_output_rejects_view(tmp_path):
    out_dir = tmp_path / "outputs"
    out_dir.mkdir()
    with pytest.raises(ValueError, match="txt output does not support view"):
        ServeOutputConfig(
            transport="fs",
            format="txt",
            view="raw",
            directory=out_dir,
        )
