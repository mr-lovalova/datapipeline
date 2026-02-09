from pathlib import Path

import pytest
from datapipeline.cli.commands.run import _build_cli_output_config


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
