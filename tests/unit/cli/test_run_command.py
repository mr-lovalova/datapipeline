from pathlib import Path

import pytest

from datapipeline.cli.commands.run import _build_cli_output_config


def test_build_cli_output_config_payload_only() -> None:
    config, payload = _build_cli_output_config(
        transport=None,
        fmt=None,
        directory=None,
        payload="VECTOR",
    )
    assert config is None
    assert payload == "vector"


def test_build_cli_output_config_fs_requires_directory() -> None:
    with pytest.raises(SystemExit) as exc:
        _build_cli_output_config(
            transport="fs",
            fmt="jsonl",
            directory=None,
            payload=None,
        )
    assert exc.value.code == 2


def test_build_cli_output_config_stdout_rejects_directory() -> None:
    with pytest.raises(SystemExit) as exc:
        _build_cli_output_config(
            transport="stdout",
            fmt="jsonl",
            directory="out",
            payload=None,
        )
    assert exc.value.code == 2


def test_build_cli_output_config_fs_populates_output() -> None:
    config, payload = _build_cli_output_config(
        transport="fs",
        fmt="jsonl",
        directory="artifacts",
        payload="sample",
    )
    assert config is not None
    assert config.transport == "fs"
    assert config.format == "jsonl"
    assert config.directory == Path("artifacts").resolve()
    assert config.payload == "sample"
    assert payload is None
